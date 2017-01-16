import java.io.InvalidObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

public class Cache extends UnicastRemoteObject implements Cloud.DatabaseOps {
    private static ConcurrentHashMap<String, String> hm = new ConcurrentHashMap<String, String>();
    private static Cloud.DatabaseOps db = null;
    private final ServerLib SL;

    protected Cache(ServerLib SL) throws RemoteException, InvalidObjectException {
        super();
        this.SL = SL;
        if(SL==null){
            throw new InvalidObjectException("Error: SL is null");
        }
        db=SL.getDB();
        if(db==null){
            throw new InvalidObjectException("Error: db is null");
        }
    }

    // Return the value from cache if it's available in the cache.
    // Otherwise, get the value from database and save it in the cache.
    @Override
    public String get(String key) throws RemoteException {
        String value = null;
        key=key.trim();
        if (key != null) {
            if (hm.containsKey(key)) {
                value = hm.get(key);
            } else {
                value = db.get(key);
                hm.put(key, value);
            }
        }
        return value;
    }

    // Call set method, and update the cache.
    @Override
    public boolean set(String key, String value, String auth) throws RemoteException {
        key=key.trim();
        boolean ret = db.set(key, value, auth);
        if (ret) {
            hm.put(key, value);
            return true;
        }
        return false;
    }

    // Call transaction method, and update the cache.
    @Override
    public boolean transaction(String item, float price, int qty) throws RemoteException {
        item=item.trim();
        boolean ret = db.transaction(item, price, qty);
        if (ret) {
            int q = Integer.parseInt(hm.get(item+"_qty"));
            q-=qty;
            hm.put((item + "_qty"), Integer.toString(q));
            return true;
        }
        return false;
    }
}
