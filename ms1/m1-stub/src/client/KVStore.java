package client;

import java.net.Socket;
import java.util.Set;
import java.io.OutputStream;
import java.io.InputStream;
//import java.io.ObjectOutputStream;
//import java.io.ObjectInputStream;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.HashSet;

import app_kvClient.KVClient;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;
import shared.messages.TextMessage;

public class KVStore implements KVCommInterface {

	private final String serverAddr;
	private final int serverPort;
	private Socket clientSocket;
	private OutputStream outputStream;
	private InputStream inputStream;
//	private ObjectOutputStream objectOutputStream;
//	private ObjectInputStream objectInputStream;
	private Set<KVClient> clients;
	private boolean connected = false;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * 1024;

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
		outputStream = clientSocket.getOutputStream();
		inputStream = clientSocket.getInputStream();
//		objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
//		objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
		clients = new HashSet<KVClient>();
		connected = true;
	}

	@Override
	public void disconnect() {
		try {
//			if (objectOutputStream != null)
//				objectOutputStream.close();
//			if (objectInputStream != null)
//				objectInputStream.close();
			if (clientSocket != null)
				clientSocket.close();
		} catch (IOException ignored) {}
	}

	@Override
	public Message put(String key, String value) throws Exception {
		writeOutputStream(new TextMessage("put " + key + " " + value));
		String[] token = readInputStream().getTextMessage().split("\\s+");
		StringBuilder val = new StringBuilder();
		for (int i=2; i<token.length; ++i) {
			val.append(token[i]);
			if (i != token.length - 1)
				val.append(" ");
		}
		return new Message(StatusType.valueOf(token[0]), token[1], val.toString());
//		objectOutputStream.writeObject(new Message(StatusType.PUT, key, value));
//		return (Message) objectInputStream.readObject();
	}

	@Override
	public Message get(String key) throws Exception {
		writeOutputStream(new TextMessage("get " + key));
		String[] token = readInputStream().getTextMessage().split("\\s+");
		StringBuilder val = new StringBuilder();
		for (int i=2; i<token.length; ++i) {
			val.append(token[i]);
			if (i != token.length - 1)
				val.append(" ");
		}
		return new Message(StatusType.valueOf(token[0]), token[1], val.toString());
//		objectOutputStream.writeObject(new Message(StatusType.GET, key, ""));
//		return (Message) objectInputStream.readObject();
	}

	public void addClient(KVClient client) {
		clients.add(client);
	}

	public boolean isConnected() {
		return connected;
	}

	private void writeOutputStream(TextMessage msg) throws IOException {
		byte[] byteMsg = msg.getByteMessage();
		outputStream.write(byteMsg, 0, byteMsg.length);
		outputStream.flush();
	}

	private TextMessage readInputStream() throws IOException {
		byte read = (byte) inputStream.read();
		boolean drop = false;
		int i = 0;
		byte[] byteMsg = null;
		byte[] byteMessage = null;
		byte[] byteBuffer = new byte[BUFFER_SIZE];
		while (read != -1 && read != 0x0A && !drop) {
			if (i == BUFFER_SIZE) {
				if (byteMsg == null) {
					byteMessage = new byte[BUFFER_SIZE];
					System.arraycopy(byteBuffer, 0, byteMessage, 0, BUFFER_SIZE);
				} else {
					byteMessage = new byte[byteMsg.length + BUFFER_SIZE];
					System.arraycopy(byteMsg, 0, byteMessage, 0, byteMsg.length);
					System.arraycopy(byteBuffer, 0, byteMessage, byteMsg.length, BUFFER_SIZE);
				}
				byteMsg = byteMessage;
				byteBuffer = new byte[BUFFER_SIZE];
				i = 0;
			}
			byteBuffer[i++] = read;
			if (byteMsg != null && byteMessage.length + i == DROP_SIZE)
				drop = true;
			read = (byte) inputStream.read();
		}
		if (byteMsg == null) {
			byteMessage = new byte[i];
			System.arraycopy(byteBuffer, 0, byteMessage, 0, i);
		} else {
			byteMessage = new byte[byteMsg.length + i];
			System.arraycopy(byteMsg, 0, byteMessage, 0, byteMsg.length);
			System.arraycopy(byteBuffer, 0, byteMessage, byteMsg.length, i);
		}
		return new TextMessage(byteMessage);
	}
}
