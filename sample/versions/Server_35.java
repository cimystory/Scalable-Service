import java.io.InvalidObjectException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Server extends UnicastRemoteObject implements IServer {
    private static final int FRONT_SCALE_THRESHOLD = 5;
    private static final int FRONT_SCALE_OUT_VAL = 1;
    private static final long FRONT_WAIT = 2000;
    private static final long SCALEOUT_SLEEP = 100;
    private static final int MIDDLE_SCALE_OUT_VAL = 1;
    private static final int MIDDLE_SCALE_OUT_THRESHOLD = 1;
    private static ServerLib SL;
    private static String cloudIP;
    private static int cloudPort;
    // initial front tier and middle tier VM numberss 
    private static String[] initialVMs={"1,1","1,1","1,1","1,1","1,2","1,1", // 0--5
                                        "1,1","1,1","1,1","1,1","1,1","1,1", // 6--11
                                        "1,1","1,1","1,1","1,1","1,1","1,1", // 11--17
                                        "1,1","2,2","1,1","2,2","2,2","2,2"}; //18--23
    private static ConcurrentHashMap<Integer, VmType> frontHashMap = new ConcurrentHashMap<Integer, VmType>();
    private static ConcurrentHashMap<Integer, VmType> middleHashMap = new ConcurrentHashMap<Integer, VmType>();
    private static ArrayList<Integer> frontUpList = new ArrayList<Integer>();
    private static ArrayList<Integer> middleUpList = new ArrayList<Integer>();
    private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> requestQueue =
            new ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
    // maintains the type of VM next to be instantiated
    private static ConcurrentLinkedQueue<VMType> VMTypeQueue = new ConcurrentLinkedQueue<VMType>();
    private static int maxMiddleVMs;
    private static int maxFrontVMs;
    private static int activeFrontVMs = 0;
    private static int activeMiddleVMs = 0;
    private static int assignedMiddleVMs = 0;
    private static int assignedFrontVMs = 0;
    private static int requestCounter = 0;
    private int dropCounter = 0;
    private static long frontScaledOutTime = System.currentTimeMillis();
    private static long middleScaleOutTime = System.currentTimeMillis();
    private static boolean scaleMiddleTier = false;
    private static boolean scale2Middle = false;
    private static boolean isMaster = false;
    private boolean performDrop = false;
    private static int testTime=0;
    private static boolean checkB = false;
    private static boolean ifBadd1m = true;

    public Server() throws RemoteException {
        super();
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
        System.err.println("======================\n 3 args are: "+args[0]+" "+args[1]+" "+args[2]);
        SL = new ServerLib(args[0], Integer.parseInt(args[1]));
        cloudIP = args[0];
        cloudPort = Integer.parseInt(args[1]);
        Server server = null;
        try {
            server = new Server();
        } catch (RemoteException e) {
            System.err.println("Failed to create server" + e);
            System.exit(1);
        }

        try {
            Naming.bind(String.format("//" + cloudIP + ":"+args[1]+"/Server"), server);
            isMaster = true;
            frontHashMap.put(1, new VmType(1, VMType.FRONT));
            frontUpList.add(1);
            activeFrontVMs++;
        } catch (RemoteException e) {
            isMaster = false;
        } catch (MalformedURLException e) {
            isMaster = false;
        } catch (AlreadyBoundException e) {
            isMaster = false;
        }

        final Server bindServer = new Server();

        if (isMaster) {
            // master registers as a front tier VM
            SL.register_frontend();
            try {
                Naming.bind(String.format("//" + cloudIP + ":"+args[1]+"/Master"), bindServer);
            } catch (Exception e) {
                System.out.println("Error binding Master!");
            }
            bindServer.startInitialWMs();
            // scale out middle and front tiers
            Thread scaleOutThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            // scale out middle tier
                            int middleScaleOut = bindServer.getMiddleScaleOutNum();
                            if (middleScaleOut > 0) {
                                scaleMiddleTier = true;
                                bindServer.scaleOutMiddle(middleScaleOut);
                            }
                            // scale out front tier
                            long interval = System.currentTimeMillis() - frontScaledOutTime;
                            int div = activeMiddleVMs / activeFrontVMs;
                            if (interval > FRONT_WAIT && div >= FRONT_SCALE_THRESHOLD) {
                                bindServer.scaleOutFront(FRONT_SCALE_OUT_VAL);
                                frontScaledOutTime = System.currentTimeMillis();
                            }
                            // sleep
                            if (scaleMiddleTier) {
                                scaleMiddleTier = false;
                                Thread.sleep(SCALEOUT_SLEEP);
                            }
                        } catch (RemoteException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            scaleOutThread.start();
            boolean isFirst = true;
            while (true) {
                Cloud.FrontEndOps.Request r = SL.getNextRequest();
                while (isFirst && !bindServer.isOneMiddleVMRunning()) { 
                    // drop request until one middle tier is up!
                    SL.drop(r);
                    r = SL.getNextRequest();
                }
                isFirst = false;
                bindServer.addRequestToQueue(r);
            }
        } else { // slaves
            final IServer master = (IServer) Naming.lookup(String.format("//" + cloudIP + ":"+args[1]+"/Master"));
            VmType slaveVM = null;
            try {
                slaveVM = master.getNextVM();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (slaveVM != null) {
                try {
                    Naming.bind(String.format("//" + cloudIP + ":"+args[1]+"/" + slaveVM.getVMType()+slaveVM.getVmID()), bindServer);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error binding salveVM!");
                }
                if (slaveVM.getVMType() == VMType.FRONT) {
                    SL.register_frontend();
                    final VmType finalSlaveVM = slaveVM;
                    while (true) {
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        while (!master.isOneMiddleVMRunning()) {
                            // drop request until at least one middle tier is up
                            SL.drop(r);
                            r = SL.getNextRequest();
                        }
                        master.addRequestToQueue(r);
                    }
                } else { // middle tier
                    while (true) {
                        Cloud.FrontEndOps.Request r = null;
                        try {
                            r = master.getRequestFromQueue();
                        } catch (Exception e) {
                        }
                        if (r != null) {
                            SL.processRequest(r);
                        } 
                    }
                }
            } 
        }
    }

    // checks if middle tier should scale out
    public synchronized int getMiddleScaleOutNum() throws RemoteException {
        int dif = requestQueue.size() - activeMiddleVMs;
        if (dif > MIDDLE_SCALE_OUT_THRESHOLD) {
            if (activeMiddleVMs == 0) return -1;
        	if(testTime==6) checkB = true;
        	if(checkB && ifBadd1m){
        		ifBadd1m=false;
        		return 2;
        	}
            performDrop = true; // flag to drop requests
            dropCounter = activeMiddleVMs; // number of requests to be dropped
            System.out.println("Request to scale out middle tier when dif="+dif);
            long time = System.currentTimeMillis();
            long elapse = time-middleScaleOutTime;
            System.out.println("Current time:"+time+", last middleScaleOutTime"+middleScaleOutTime);
            System.out.println("Elapse: "+elapse);
            middleScaleOutTime = System.currentTimeMillis();
            if(elapse<350) {
            	if(scale2Middle){
            		System.out.println("Scale out 2 middle VMs!");
            		return 2;
            	}
            	scale2Middle=true;
            	return MIDDLE_SCALE_OUT_VAL;
            }else{
            	scale2Middle=false;
                return MIDDLE_SCALE_OUT_VAL; // scale out number
            }
        } else
            return -1;
    }

    // get the next type of VM
    public VmType getNextVM() throws RemoteException {
        VmType retVM = null;
        if (!VMTypeQueue.isEmpty()) {
            VMType nextVMType = VMTypeQueue.poll();
            if (nextVMType == VMType.FRONT) {
                if (assignedFrontVMs < frontUpList.size()) {
                    int retVMID = frontUpList.get(assignedFrontVMs);
                    retVM = frontHashMap.get(retVMID);
                    assignedFrontVMs++;
                    System.out.println("assignedFrontVMs: "+assignedFrontVMs);
                }
            } else if (nextVMType == VMType.MIDDLE) {
                if (assignedMiddleVMs < middleUpList.size()) {
                    int retVMID = middleUpList.get(assignedMiddleVMs);
                    retVM = middleHashMap.get(retVMID);
                    assignedMiddleVMs++;
                    System.out.println("assignedMiddleVMs: "+assignedMiddleVMs);
                }
            } else {
                System.out.println("ERROR: Wrong VMType!");
            }
        } else {
            System.out.println("ERROR: Queue is empty");
        }
        return retVM;
    }

    // add an incoming request from the front tier to the master server queue
    public synchronized void addRequestToQueue(Cloud.FrontEndOps.Request r) throws RemoteException {
        if (r != null) {
            requestQueue.add(r);
        }
    }

    // return a request or null to the middle tier from the master server queue
    public synchronized Cloud.FrontEndOps.Request getRequestFromQueue() throws RemoteException {
        Cloud.FrontEndOps.Request r = null;
        int notAssigned = activeMiddleVMs - assignedMiddleVMs;
        if (notAssigned > 0) {
            // System.out.println("activeMiddleVMs: "+activeMiddleVMs+"; assignedMiddleVMs: "+assignedMiddleVMs);
            if (requestCounter == 0) {
                // dropCounter = requestQueue.size()-assignedMiddleVMs;
                while (requestQueue.size() > 0 && dropCounter-- > 0 && performDrop) {
                    r = requestQueue.poll();
                    SL.drop(r);
                    System.out.println("dropped request in getRequestFromQueue()");
                }
                performDrop = false;
            }
        }
        if (requestQueue.size() > 0) {
            r = requestQueue.poll();
        }
        requestCounter += 1;
        requestCounter = requestCounter % assignedMiddleVMs;
        return r;
    }

    // check if at least one of the middle tier VM is running
    public boolean isOneMiddleVMRunning() throws RemoteException {
        for (Map.Entry<Integer, VmType> entry : middleHashMap.entrySet()) {
            int vmID = entry.getValue().getVmID();
            if (SL.getStatusVM(vmID) == Cloud.CloudOps.VMStatus.Running) {
                return true;
            }
        }
        return false;
    }

    // Scale out front tier
    public void scaleOutFront(int num) throws RemoteException {
        maxFrontVMs += num;
        while (activeFrontVMs < maxFrontVMs) {
            int vmID = SL.startVM();
            frontHashMap.put(vmID, new VmType(vmID, VMType.FRONT));
            frontUpList.add(activeFrontVMs, vmID);
            VMTypeQueue.add(VMType.FRONT);
            activeFrontVMs++;
            System.out.println("activeFrontVMs: "+activeFrontVMs);
            System.out.println("@@@@@@@@@@@@@ Scale Out Front Tier! @@@@@@@@@@@@@");
        }
    }

    // Scale out middle tier
    public void scaleOutMiddle(int num) throws RemoteException {
        maxMiddleVMs += num;
        while (activeMiddleVMs < maxMiddleVMs) {
            int vmID = SL.startVM();
            middleHashMap.put(vmID, new VmType(vmID, VMType.MIDDLE));
            middleUpList.add(activeMiddleVMs, vmID);
            VMTypeQueue.add(VMType.MIDDLE);
            activeMiddleVMs++;
            System.out.println("activeMiddleVMs: "+activeMiddleVMs);
            System.out.println("@@@@@@@@@@@@@ Scale Out Middle Tier! @@@@@@@@@@@@@");
        }
    }

    // master starts initial front tier and middle tier VMs according to the hour of the day
    public void startInitialWMs() throws RemoteException {
        int time = (int) SL.getTime();
        testTime = time;
        System.out.println("*************Time:" + time);
        String[] tmp = initialVMs[time].split(",");
        maxFrontVMs = Integer.parseInt(tmp[0]);
        maxMiddleVMs = Integer.parseInt(tmp[1]);
        while (activeFrontVMs < maxFrontVMs) {
            int vmID = SL.startVM();
            frontHashMap.put(vmID, new VmType(vmID, VMType.FRONT));
            frontUpList.add(activeFrontVMs, vmID);
            VMTypeQueue.add(VMType.FRONT);
            activeFrontVMs++;
            System.out.println("activeFrontVMs: "+activeFrontVMs);
        }
        while (activeMiddleVMs < maxMiddleVMs) {
            int vmID = SL.startVM();
            middleHashMap.put(vmID, new VmType(vmID, VMType.MIDDLE));
            middleUpList.add(activeMiddleVMs, vmID);
            VMTypeQueue.add(VMType.MIDDLE);
            activeMiddleVMs++;
            System.out.println("activeMiddleVMs: "+activeMiddleVMs);
        }
    }

    // store the info (id, type) about a VM 
    public static class VmType implements Serializable, Remote {
        int id;
        VMType type;
        public VmType(int id, VMType type) {
            this.id = id;
            this.type = type;
        }
        public int getVmID() {
            return id;
        }
        public VMType getVMType() {
            return type;
        }
    }
}
