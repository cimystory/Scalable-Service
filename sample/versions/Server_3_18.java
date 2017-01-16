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
    private static final long FRONT_TIER_SLEEP_TIME = 1000;
    private static final long SCALEOUT_SLEEP = 100;
    private static final int MIDDLE_SCALE_OUT_THRESHOLD = 1;
    private static final int FAST_MODE_RATE = 100;
    private static final int SCALE_IN_RATE = 400;
    private static final int MIN_FRONT = 1;
    private static final int MIN_MIDDLE = 2;
    private static final int SCALE_FAST = 3;
    private static final int SCLAE_NORMAL = 1;

    private static ServerLib SL;
    private static String cloudIP;
    private static int cloudPort;
    private static Cloud.DatabaseOps cache;
    // initial front tier and middle tier VM numberss 
    private static String[] initialVMs={"1,1","1,1","1,1","1,1","1,2","1,1", // 0--5
                                        "1,1","1,1","1,1","1,1","1,1","1,1", // 6--11
                                        "1,1","1,1","1,1","1,1","1,1","1,1", // 11--17
                                        "1,1","2,2","1,2","2,2","2,2","2,2"}; //18--23
    private static ConcurrentHashMap<Integer, VmType> frontHashMap = new ConcurrentHashMap<Integer, VmType>();
    private static ConcurrentHashMap<Integer, VmType> middleHashMap = new ConcurrentHashMap<Integer, VmType>();
    private static ArrayList<Integer> frontUpList = new ArrayList<Integer>();
    private static ArrayList<Integer> middleUpList = new ArrayList<Integer>();
    private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> requestQueue =
            new ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
    // store the type of VM next to be instantiated
    private static ConcurrentLinkedQueue<VMType> VMTypeQueue = new ConcurrentLinkedQueue<VMType>();
    private static int maxMiddleVMs;
    private static int maxFrontVMs;
    private static int activeFrontVMs = 0;
    private static int activeMiddleVMs = 0;
    private static int assignedMiddleVMs = 0;
    private static int assignedFrontVMs = 0;
    private static int requestCounter = 0;
    private static int dropCounter = 0;
    private static long frontScaledOutTime = System.currentTimeMillis();
    private static long middleScaleOutTime = System.currentTimeMillis();
    private static boolean scaleMiddleTier = false;
    private static boolean isMaster = false;
    private static boolean performDrop = false;
    private static boolean scaleinFront = false;
    private static boolean scaleinMiddle = false;
    private static boolean fastmode = false;

    public Server() throws RemoteException {
        super();
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
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
            // Server binds once
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
                cache = new Cache(SL);
                Naming.bind(String.format("//" + cloudIP + ":"+args[1]+"/Cache"), cache);
            } catch (Exception e) {
                System.out.println("Error binding Master!");
            }
            bindServer.startInitialWMs();
            // A thread to scale out middle and front tiers
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
            long rt=0, now, t, sum=0; 
            long startT = System.currentTimeMillis();
            int n=0;
            while (true) {
                // Get the next request. Drop if no middle VM is running, else add it to the request queue.
                Cloud.FrontEndOps.Request r = SL.getNextRequest();
                while (isFirst && !bindServer.isOneMiddleVMRunning()) { 
                    // drop request until one middle tier is up!
                    SL.drop(r);
                    r = SL.getNextRequest();
                }
                if(isFirst){
                    isFirst = false;
                    rt = System.currentTimeMillis();
                }else{
                    now = System.currentTimeMillis();
                    t = now-rt;
                    sum+=t;
                    n++;
                    if(n==10){
                        // compute the average of every 10 requests rate
                        System.out.println("average rate: "+sum/10+", current time: "+(now-startT));
                        if(sum/10<FAST_MODE_RATE){
                            fastmode=true;
                        }
                        if(sum/10>SCALE_IN_RATE && (now-startT)>30000){
                            scaleinFront=true;
                            scaleinMiddle=true;
                        }
                        n=0;
                        sum=0;
                    }
                    rt=now;
                    bindServer.addRequestToQueue(r);
                }
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
                if (slaveVM.getVMType() == VMType.FRONT) { // front tier
                    SL.register_frontend();
                    final VmType finalSlaveVM = slaveVM;

                    // A thread to scale down front tiers
                    Thread frontTierScaleDownThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            while(true){
                                try {
                                    if (master.checkScaleInFront()) {
                                        SL.interruptGetNext();
                                        UnicastRemoteObject.unexportObject(bindServer, true);
                                        SL.shutDown();
                                        System.exit(0);
                                    }
                                    Thread.sleep(FRONT_TIER_SLEEP_TIME);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                        }
                    });
                    frontTierScaleDownThread.start();

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
                    cache = (Cloud.DatabaseOps) Naming.lookup(String.format("//" + cloudIP + ":"+args[1]+"/Cache"));
                    while (true) {
                        Cloud.FrontEndOps.Request r = null;
                        try {
                            r = master.getRequestFromQueue();
                        } catch (Exception e) {
                        }
                        if (r != null) {
                            if(cache!=null){
                                SL.processRequest(r, cache);
                            }else{
                                System.out.println("Error: Cache is NULL");
                            }
                        } else {
                            // scale down middle tier
                            if (master.checkScaleInMiddle()) {
                                try {
                                    UnicastRemoteObject.unexportObject(bindServer, true);
                                    SL.shutDown();
                                    activeMiddleVMs--;
                                    System.exit(0);
                                } catch (Exception e) {
                                }
                            }
                        }
                    }
                }
            } 
        }
    }

    // Check if front tier should scale in
    public boolean checkScaleInFront() throws RemoteException{
        if(scaleinFront && activeFrontVMs>MIN_FRONT){
            activeFrontVMs--;
            return true;
        }else{
            return false;
        }
    }

    // Check if front tier should scale in
    public boolean checkScaleInMiddle() throws RemoteException{
        if(scaleinMiddle && activeMiddleVMs>MIN_MIDDLE){
            activeMiddleVMs--;
            return true;
        }else{
            return false;
        }
    }

    // Checks if middle tier should scale out, return the number
    public synchronized int getMiddleScaleOutNum() throws RemoteException {
        int dif = requestQueue.size() - activeMiddleVMs;
        if (dif > MIDDLE_SCALE_OUT_THRESHOLD) {
            if (activeMiddleVMs == 0) return -1;
            performDrop = true; // flag to drop requests
            dropCounter = activeMiddleVMs; // number of requests to be dropped
            middleScaleOutTime = System.currentTimeMillis();
            if(fastmode && activeMiddleVMs<8) {
                System.out.println("Scale out 3 middle VMs!");
                return SCALE_FAST;
            }
            return SCLAE_NORMAL;
        } else
            return -1;
    }

    // Get the next type of VM in MTypeQueue
    public VmType getNextVM() throws RemoteException {
        VmType retVM = null;
        if (!VMTypeQueue.isEmpty()) {
            VMType nextVMType = VMTypeQueue.poll();
            if (nextVMType == VMType.FRONT) {
                if (assignedFrontVMs < frontUpList.size()) {
                    int retVMID = frontUpList.get(assignedFrontVMs);
                    retVM = frontHashMap.get(retVMID);
                    assignedFrontVMs++;
                }
            } else if (nextVMType == VMType.MIDDLE) {
                if (assignedMiddleVMs < middleUpList.size()) {
                    int retVMID = middleUpList.get(assignedMiddleVMs);
                    retVM = middleHashMap.get(retVMID);
                    assignedMiddleVMs++;
                }
            } else {
                System.out.println("ERROR: Wrong VMType!");
            }
        } else {
            System.out.println("ERROR: Queue is empty");
        }
        return retVM;
    }

    // Add an incoming request from the front tier to the master server queue
    public synchronized void addRequestToQueue(Cloud.FrontEndOps.Request r) throws RemoteException {
        if (r != null) {
            requestQueue.add(r);
        }
    }

    // Return a request or null to the middle tier from the master server queue
    public synchronized Cloud.FrontEndOps.Request getRequestFromQueue() throws RemoteException {
        Cloud.FrontEndOps.Request r = null;
        int notAssigned = activeMiddleVMs - assignedMiddleVMs;
        if (notAssigned > 0) {
            if (requestCounter == 0) {
                while (requestQueue.size() > 0 && dropCounter-- > 0 && performDrop) {
                    r = requestQueue.poll();
                    SL.drop(r);
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

    // Check if at least one of the middle tier VM is running
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

    // Master starts initial front tier and middle tier VMs according to the hour of the day
    public void startInitialWMs() throws RemoteException {
        int time = (int) SL.getTime();
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
        }
    }

    // Store the info (id, type) of a VM 
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
