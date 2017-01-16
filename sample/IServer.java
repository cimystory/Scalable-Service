import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IServer extends Remote {
    public enum VMType {
        MASTER, FRONT, MIDDLE;
    }
    public Server.VmType getNextVM() throws RemoteException;
    public void startInitialWMs() throws RemoteException;
    public boolean isOneMiddleVMRunning() throws RemoteException;
    public void addRequestToQueue(Cloud.FrontEndOps.Request request) throws RemoteException;
    public Cloud.FrontEndOps.Request getRequestFromQueue() throws RemoteException;
    public int getMiddleScaleOutNum() throws RemoteException;
    public void scaleOutMiddle(int scale) throws RemoteException;
    public void scaleOutFront(int scale) throws RemoteException;
    public boolean checkScaleInFront() throws RemoteException;
    public boolean checkScaleInMiddle() throws RemoteException;
}
