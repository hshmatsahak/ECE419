package client;

import java.net.Socket;
import java.util.Set;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.HashSet;

import app_kvClient.KVClient;
import shared.messages.Message;

public class KVStore implements KVCommInterface {

	private final String serverAddr;
	private final int serverPort;
	private Socket clientSocket;
	private Set<KVClient> clients;
	private boolean connected = false;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		serverAddr = address;
		serverPort = port;
	}

	@Override
	public void connect() throws UnknownHostException, IOException {
		clientSocket = new Socket(serverAddr, serverPort);
		clients = new HashSet<KVClient>();
		connected = true;
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
	}

	@Override
	public Message put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public void addClient(KVClient client) {
		clients.add(client);
	}

	public boolean isConnected() {
		return connected;
	}
}
