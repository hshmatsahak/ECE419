package app_kvServer;

import java.net.ServerSocket;
import java.util.concurrent.locks.ReentrantLock;
import java.net.Socket;
import java.io.File;
import java.util.Scanner;
import java.io.FileWriter;
import java.io.IOException;
import java.net.BindException;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import logger.LogSetup;

public class KVServer extends Thread implements IKVServer {

	private static final Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVServer> ";
	private int serverPort;
	private String serverStorePath;
	private boolean online;
	private ServerSocket serverSocket;
	final ReentrantLock serverLock = new ReentrantLock();

	public KVServer(int port, String storeDir) {
		serverPort = port;
		serverStorePath = storeDir + "/";
	}

	public static void main(String[] args) {
		handleArgs(args);
	}

	private static void handleArgs(String[] args) {
		if (args.length == 0)
			pexit("No Arguments");
		int port = -1;
		String storeDir = null;
		String logDir = "logs";
		Level logLevel = Level.ALL;
		for (int i=0; i<args.length; ++i) {
			switch (args[i]) {
			case "-p":
				if (port != -1)
					pexit("Ambiguous Port Arguments");
				if (++i == args.length)
					pexit("No Port Argument");
				try {
					port = Integer.parseInt(args[i]);
				} catch (NumberFormatException nfe) {
					pexit("Invalid Port Argument");
				}
				break;
			case "-a":
				if (++i == args.length)
					pexit("No Listen Address Argument");
				pexit("Invalid Listen Address Argument");
				break;
			case "-d":
				if (storeDir != null)
					pexit("Ambiguous Storage Path Arguments");
				if (++i == args.length)
					pexit("No Storage Path Argument");
				storeDir = args[i];
				break;
			case "-l":
				if (!logDir.equals("logs"))
					pexit("Ambiguous Log Directory Argument");
				if (++i == args.length)
					pexit("No Log Directory Argument");
				logDir = args[i];
				break;
			case "-ll":
				if (!logLevel.equals("ALL"))
					pexit("Ambiguous Log Level Argument");
				if (++i == args.length)
					pexit("No Log Level Argument");
				if (args[i].equals("DEBUG"))
					logLevel = Level.DEBUG;
				else if (args[i].equals("INFO"))
					logLevel = Level.INFO;
				else if (args[i].equals("WARN"))
					logLevel = Level.WARN;
				else if (args[i].equals("ERROR"))
					logLevel = Level.ERROR;
				else if (args[i].equals("FATAL"))
					logLevel = Level.FATAL;
				else if (args[i].equals("OFF"))
					logLevel = Level.OFF;
				else
					pexit("Invalid Log Level Argument");
				break;
			case "-h":
				System.exit(0);
			default:
				pexit("Invalid Argument: " + args[i]);
			}
		}
		if (port == -1 || storeDir == null)
			pexit("Too Few Arguments");
		try {
			new LogSetup(logDir + "/server.log", logLevel);
		} catch (IOException e) {
			e.printStackTrace();
			pexit("Initialize Server Log");
		}
		new KVServer(port, storeDir).start();
	}

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

//	public static void main(String[] args) {
//		if (args.length != 1) {
//			System.out.println("KVServer> Error: Invalid Argument Count!");
//			System.exit(1);
//		} else {
//			try {
//				int serverPort = Integer.parseInt(args[0]);
//				new KVServer(serverPort, -1, CacheStrategy.None.toString()).start();
//			} catch (NumberFormatException ioe) {
//				System.out.println("KVServer> Error: Invalid Server Port!");
//				System.exit(1);
//			}
//		}
//	}

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
		File kvFile = new File(serverStorePath + key);
		return kvFile.exists();
	}

	@Override
    public boolean inCache(String key){
		return false;
	}

	@Override
    public String getKV(String key) throws Exception {
		logger.info("getting " + key + "...");
		File kvFile = new File(serverStorePath + key);
		if (!kvFile.exists()) {
			logger.error(key + " Does Not Exist!");
			throw new Exception("File Does Not Exist!");
		}
		Scanner kvFileScanner = new Scanner(kvFile);
		String val = kvFileScanner.nextLine();
		kvFileScanner.close();
		logger.info("Corresponding: " + val);
		return val;
	}

	@Override
    public void putKV(String key, String value) throws Exception {
		File kvFile = new File(serverStorePath + key);
		if (value.equals("null")) {
			if (kvFile.exists()) {
				kvFile.delete();
				logger.info("Deleted " + key);
			} else {
				logger.error(key + " Does Not Exist!");
				throw new Exception("File Does Not Exist!");
			}
		} else {
			if (kvFile.exists()) {
				kvFile.delete();
				logger.info("Updating " + key + " Corresponding: " + value + " ...");
			} else
				logger.info("Inserting " + key + " Corresponding: " + value + " ...");
			try {
				kvFile.createNewFile();
				FileWriter kvFileWriter = new FileWriter(kvFile);
				kvFileWriter.write(value);
				kvFileWriter.close();
				logger.info("Done!");
			} catch (Exception e) {
				logger.error("Error Creating/Writing File!");
				throw new Exception("Error Creating/Writing File!");
			}
		}
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

	private boolean initializeServer() {
		System.out.println("KVServer> Initializing Server...");
		try {
			serverSocket = new ServerSocket(serverPort);
			System.out.println("KVServer> Server Online! Port " + serverPort);
			return true;
		} catch (IOException e) {
			System.out.println("KVServer> Error: Socket Bind Failed!");
			if (e instanceof BindException)
				System.out.println("KVServer> Port " + serverPort + " Unavailable!");
			return false;
		}
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}

	private static void pexit(String str) {
		System.out.println(PROMPT + "Error: " + str + "!");
		System.exit(1);
	}
}
