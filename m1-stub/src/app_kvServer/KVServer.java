package app_kvServer;

import java.net.ServerSocket;
import java.util.concurrent.locks.ReentrantLock;
import java.net.Socket;
import java.io.File;
import java.util.Scanner;
import java.io.FileWriter;
import java.io.IOException;
import java.net.BindException;

public class KVServer extends Thread implements IKVServer {

	private int serverPort;
	private boolean online;
	private ServerSocket serverSocket;
	final ReentrantLock serverLock = new ReentrantLock();
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		serverPort = port;
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("KVServer> Error: Invalid Argument Count!");
			System.exit(1);
		} else {
			try {
				int serverPort = Integer.parseInt(args[0]);
				new KVServer(serverPort, -1, CacheStrategy.None.toString()).start();
			} catch (NumberFormatException ioe) {
				System.out.println("KVServer> Error: Invalid Server Port!");
				System.exit(1);
			}
		}
	}
	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public boolean inStorage(String key) {
		File kvFile = new File("../storage/" + key);
		return kvFile.exists();
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String getKV(String key) throws Exception {
		File kvFile = new File("../storage/" + key);
		if (!kvFile.exists())
			throw new Exception();
		Scanner kvFileScanner = new Scanner(kvFile);
		String val = kvFileScanner.nextLine();
		kvFileScanner.close();
		return val;
	}

	@Override
    public void putKV(String key, String value) throws Exception {
		File kvFile = new File("../storage/" + key);
		if (kvFile.exists())
			kvFile.delete();
		if (value == null)
			return;
		kvFile.createNewFile();
		FileWriter kvFileWriter = new FileWriter(kvFile);
		kvFileWriter.write(value);
		kvFileWriter.close();
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
    public void run() {
		online = initializeServer();
		if (serverSocket == null) {
			System.out.println("KVServer> Error: Connection Lost!");
			return;
		}
		while (online) {
			try {
				Socket clientSocket = serverSocket.accept();
				ClientConnection clientConnection = new ClientConnection(clientSocket);
				clientConnection.addServer(this);
				new Thread(clientConnection).start();
			} catch (IOException e) {
//				logger.error("Error: New Client Connection Establishment Failed!");
			}
		}
		System.out.println("Closing Server...");
//		logger.info("Server Closed");
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}

	private boolean initializeServer() {
		System.out.println("KVServer> Establishing Connection...");
		try {
			serverSocket = new ServerSocket(serverPort);
			System.out.println("KVServer> Connection Established! Port " + serverPort);
			return true;
		} catch (IOException e) {
			System.out.println("KVServer> Error: Socket Bind Failed!");
			if (e instanceof BindException)
				System.out.println("KVServer> Port " + serverPort + " Unavailable!");
			return false;
		}
	}
}
