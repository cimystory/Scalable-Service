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
    private static final long FRONT_WAIT_TIME = 2000;
    private static final long MIDDLE_SCALE_IN_THRESHOLD = 3000;
    private static final long MIDDLE_SLEEP_TIME = 1000;
    private static final int MIDDLE_SCALE_OUT_VAL = 2;
    private static final int MIDDLE_SCALE_OUT_THRESHOLD = 0;
    private static final int MIN_REQUIRED_FRONTS = 2;
    private static final int MIN_REQUIRED_MIDDLES = 1;
    private static ServerLib SL;
    private static String cloudIP;
    private static int cloudPort;
    // initial front tier and middle tier VM numberss 
    private static String[] initialVMs={"2,2","2,2","2,2","2,2","2,2","2,2", // 0--5
                                        "2,2","2,2","2,2","2,2","2,2","2,2", // 6--11
                                        "2,2","2,2","2,2","2,2","2,2","2,2", // 12--17
                                        "2,2","2,3","2,2","2,2","2,2","2,2"}; //18--23
    private static ConcurrentHashMap<Integer, VmType> frontHashMap = new ConcurrentHashMap<Integer, VmType>();
    private static ConcurrentHashMap<Integer, VmType> middleHashMap = new ConcurrentHashMap<Integer, VmType>();
    private static ArrayList<Integer> frontUpList = new ArrayList<Integer>();
    private static ArrayList<Integer> middleUpList = new ArrayList<Integer>();
    private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> requestHandlingQueue =
            new ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
    // maintains the type of VM next to be instantiated
    private static ConcurrentLinkedQueue<VMType> VMTypeQueue = new ConcurrentLinkedQueue<VMType>();
    private static ArrayList<Integer> completedMiddleTierList = new ArrayList<Integer>();
    private static ArrayList<Integer> completedFrontTierList = new ArrayList<Integer>();
    private static int maxMiddleVMs;
    private static int maxFrontVMs;
    private static int activeFrontVMs = 0;
    private static int activeMiddleVMs = 0;
    private static int bootedMiddleVMs = 0;
    private static int assignedMiddleTiers = 0;
    private static int assignedFrontTiers = 0;
    private static int requestCounter = 0;
    private int dropCounter = 0;
    private static long frontScaledOutTime = System.currentTimeMillis();
    private static boolean MiddleScalesOut = false;
    private static boolean isMaster = false;
    private boolean performDrop = false;

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
            // master registers as a front end VM
            SL.register_frontend();
            try {
                Naming.bind(String.format("//" + cloudIP + ":"+args[1]+"/Master"), bindServer);
            } catch (Exception e) {
                System.out.println("Error binding Master!");
            }
            bindServer.startInitialWMs();
            // control scale out middle and front tiers
            Thread scaleOutThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            // scale out middle tier
                            int middleScaleOut = bindServer.numberOfRequestsAboveThreshold();
                            if (middleScaleOut > 0) {
                                MiddleScalesOut = true;
                                bindServer.scaleOutMiddle(middleScaleOut);
                            }
                            // scale out front tier
                            long interval = System.currentTimeMillis() - frontScaledOutTime;
                            int div = ((activeMiddleVMs - completedMiddleTierList.size()) / activeFrontVMs);
                            if (interval > FRONT_WAIT_TIME && div >= FRONT_SCALE_THRESHOLD) {
                                bindServer.scaleOutFront(FRONT_SCALE_OUT_VAL);
                                frontScaledOutTime = System.currentTimeMillis();
                            }
                            // sleep
                            if (MiddleScalesOut) {
                                MiddleScalesOut = false;
                                Thread.sleep(MIDDLE_SLEEP_TIME);
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
                    Naming.bind(String.format("//" + cloudIP + ":"+args[1]+"/" + slaveVM.getVmID() + " " + slaveVM.getVMType()), bindServer);
                } catch (Exception e) {
                    System.out.println("Error binding salveVM!");
                }
                if (slaveVM.getVMType() == VMType.FRONT) {
                    SL.register_frontend();
                    final VmType finalSlaveVM = slaveVM;

                    // scale in front tiers
                    Thread scaleIn = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                if (master.shutDownFrontTierCheck() > 0) {
                                    SL.interruptGetNext();
                                    SL.unregister_frontend();
                                    master.frontTierShutdown(finalSlaveVM.getVmID());
                                    UnicastRemoteObject.unexportObject(bindServer, true);
                                    SL.shutDown();
                                    System.exit(0);
                                }
                            } catch (NoSuchObjectException e) {
                                e.printStackTrace();
                            } catch (RemoteException e) {
                                e.printStackTrace();
                            } 
                        }
                    });
                    scaleIn.start();

                    while (true) {
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        while (!master.isOneMiddleVMRunning()) {
                            // drop request until one middle tier is up
                            SL.drop(r);
                            r = SL.getNextRequest();
                        }
                        master.addRequestToQueue(r);
                    }
                } else { // middle tier
                    master.increaseMiddleTier(1);
                    long lastProcessedTimeOnMiddleTier = System.currentTimeMillis();
                    // to make sure that the middle tier has processed atleast one request before shutting down
                    boolean processedAtleastOnece = false;
                    while (true) {
                        Cloud.FrontEndOps.Request r = null;
                        try {
                            r = master.getRequestFromQueue();
                        } catch (Exception e) {
                        }
                        if (null != r) {
                            lastProcessedTimeOnMiddleTier = System.currentTimeMillis(); // update the last processed time
                            bindServer.masterProcessRequest(r);
                            processedAtleastOnece = true;
                        } else {
                            // scale down middle tier
                            try {
                                if (processedAtleastOnece) {
                                    long thresholdDiff = System.currentTimeMillis() - lastProcessedTimeOnMiddleTier;
                                    if (thresholdDiff > MIDDLE_SCALE_IN_THRESHOLD && master.shutDownMiddleTierCheck()) {
                                        master.middleTierShutdown(slaveVM.getVmID());
                                        UnicastRemoteObject.unexportObject(bindServer, true);
                                        SL.shutDown();
                                        System.exit(0);
                                    }
                                }
                            } catch (Exception e) {
                            }
                        }
                    }
                }
            } 
        }
    }

    // check if middle tier VM should be shutdown
    public boolean shutDownMiddleTierCheck() throws RemoteException {
        int dif = bootedMiddleVMs - completedMiddleTierList.size();
        return (dif > MIN_REQUIRED_MIDDLES) ? true : false;
    }

    // check if front tier VM should be shutdown
    public int shutDownFrontTierCheck() throws RemoteException {
        int dif = activeMiddleVMs - completedMiddleTierList.size() -
                ((activeFrontVMs - completedFrontTierList.size()) * FRONT_SCALE_THRESHOLD);
        if (dif < 0 && activeFrontVMs > MIN_REQUIRED_FRONTS) {
            return 1;
        }
        return -1;
    }

    // increase middle tier VM number
    public void increaseMiddleTier(int num) throws RemoteException {
        bootedMiddleVMs += num;
    }

    // checks if middle tier should scale out
    public synchronized int numberOfRequestsAboveThreshold() throws RemoteException {
        int base = activeMiddleVMs - completedMiddleTierList.size();
        int dif = (requestHandlingQueue.size()) - base;
        if (dif > MIDDLE_SCALE_OUT_THRESHOLD) {
            if (base == 0) return -1;
            int value = (int) Math.ceil(dif/(base));
            if (value > 0) {
                performDrop = true; // flag to drop requests or not
                dropCounter = base; // number of requests to be dropped
                return MIDDLE_SCALE_OUT_VAL; // scale out
            }
            return -1;
        } else
            return -1;
    }

    // update completedFrontTierList
    public void frontTierShutdown(int vmID) throws RemoteException {
        if (completedFrontTierList != null) {
            completedFrontTierList.add(vmID);
        }
    }

    // update completedMiddleTierList
    public void middleTierShutdown(int vmID) throws RemoteException {
        if (completedMiddleTierList != null) {
            completedMiddleTierList.add(vmID);
        }
    }

    // get the next type of VM
    public VmType getNextVM() throws RemoteException {
        VmType retVM = null;
        if (!VMTypeQueue.isEmpty()) {
            VMType nextVMType = VMTypeQueue.poll();
            if (nextVMType == VMType.FRONT) {
                if (assignedFrontTiers < frontUpList.size()) {
                    int retVMID = frontUpList.get(assignedFrontTiers);
                    retVM = frontHashMap.get(retVMID);
                    assignedFrontTiers++;
                }
            } else if (nextVMType == VMType.MIDDLE) {
                if (assignedMiddleTiers < middleUpList.size()) {
                    int retVMID = middleUpList.get(assignedMiddleTiers);
                    retVM = middleHashMap.get(retVMID);
                    assignedMiddleTiers++;
                }
            } else {
                System.out.println("ERROR: Wrong VMType!");
            }
        } else {
            System.out.println("ERROR: Queue is empty");
        }
        return retVM;
    }

    // process the request, called by the middle tier
    public void masterProcessRequest(Cloud.FrontEndOps.Request r) throws RemoteException {
        if (r != null) SL.processRequest(r);
    }

    // add an incoming request from the front tier to the master server queue
    public synchronized void addRequestToQueue(Cloud.FrontEndOps.Request r) throws RemoteException {
        if (r != null) {
            requestHandlingQueue.add(r);
        }
    }

    // return a request or null to the middle tier from the master server queue
    public synchronized Cloud.FrontEndOps.Request getRequestFromQueue() throws RemoteException {
        Cloud.FrontEndOps.Request r = null;
        int yetToBoot = activeMiddleVMs - bootedMiddleVMs;
        if (yetToBoot > 0) {
            if (requestCounter == 0) {
                while (requestHandlingQueue.size() > 0 && dropCounter-- > 0 && performDrop) {
                    r = requestHandlingQueue.poll();
                    SL.drop(r);
                }
                performDrop = false;
            }
        }
        if (requestHandlingQueue.size() > 0) {
            r = requestHandlingQueue.poll();
        }
        requestCounter += 1;
        requestCounter = requestCounter % bootedMiddleVMs;
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
        }
    }

    // master starts initial front tier and middle tier VMs according to the hour of the day
    public void startInitialWMs() throws RemoteException {
        int time = (int) SL.getTime();
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
        }
        while (activeMiddleVMs < maxMiddleVMs) {
            int vmID = SL.startVM();
            middleHashMap.put(vmID, new VmType(vmID, VMType.MIDDLE));
            middleUpList.add(activeMiddleVMs, vmID);
            VMTypeQueue.add(VMType.MIDDLE);
            activeMiddleVMs++;
            bootedMiddleVMs++;
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
