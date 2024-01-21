package app_kvClient;

import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {

    private KVStore kvStore = null;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        kvStore = new KVStore(hostname, port);
        try {
            System.out.println("KVClient> Establishing Connection...");
            kvStore.connect();
            System.out.println("KVClient> Connection Established!");
        } catch (Exception e) {
            System.out.println("KVClient> Connection Establishment Failed!");
        }
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }
}
