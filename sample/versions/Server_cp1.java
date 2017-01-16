/* Sample code for basic Server */

class StartVM implements Runnable{
    int time;
    ServerLib SL;
    public StartVM(ServerLib SL, int time){
        this.SL=SL;
        this.time=time-660;
    }
    @Override
    public void run(){
        try{
            Thread.sleep(time);
        }catch(Exception e){
            System.out.println("failed to sleep");
        }
        int id = SL.startVM();
    }
}

public class Server {
    public static void main ( String args[] ) throws Exception {
        if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
        System.err.println("======================\n 3 args are: "+args[0]+" "+args[1]+" "+args[2]);
        ServerLib SL = new ServerLib( args[0], Integer.parseInt(args[1]) );
        
        // register with load balancer so requests are sent to this server
        SL.register_frontend();
        int t=(int) SL.getTime();
        int time=0;
        if((t>=0 && t<2) || (t>6 && t<7)){
            time=2000;
        }else if(t>=2 && t<=6){
            time=4660;
        }else if(t>=11 && t<22){
            time=660;
        }else{
            time=1000;
        }

        Thread th = new Thread(new StartVM(SL, time));
        th.start();
        // SL.startVM();


        // main loop
        while (true) {
            Cloud.FrontEndOps.Request r = SL.getNextRequest();
            SL.processRequest( r );
        }
    }
}