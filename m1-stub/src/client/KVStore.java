package client;

import java.net.Socket;
import java.util.Set;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.HashSet;

import app_kvClient.KVClient;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private final String serverAddr;
	private final int serverPort;
	private Socket clientSocket;
	private ObjectOutputStream objectOutputStream;
	private ObjectInputStream objectInputStream;
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
		objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
		objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
		clients = new HashSet<KVClient>();
		connected = true;
	}

	@Override
	public void disconnect() {
		try {
			if (objectOutputStream != null)
				objectOutputStream.close();
			if (objectInputStream != null)
				objectInputStream.close();
			if (clientSocket != null)
				clientSocket.close();
		} catch (IOException ignored) {}
	}

	@Override
	public Message put(String key, String value) throws Exception {
		objectOutputStream.writeObject(new Message(StatusType.PUT, key, value));
		return (Message) objectInputStream.readObject();
	}

	@Override
	public Message get(String key) throws Exception {
		objectOutputStream.writeObject(new Message(StatusType.GET, key, null));
		return (Message) objectInputStream.readObject();
	}

	public void addClient(KVClient client) {
		clients.add(client);
	}

	public boolean isConnected() {
		return connected;
	}
}
