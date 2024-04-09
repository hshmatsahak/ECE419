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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;

import app_kvClient.KVClient;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;
import shared.messages.TextMessage;

public class KVStore implements KVCommInterface {

	private String serverAddr;
	private int serverPort;
	private Socket clientSocket;
	private OutputStream outputStream;
	private InputStream inputStream;
//	private ObjectOutputStream objectOutputStream;
//	private ObjectInputStream objectInputStream;
	private Set<KVClient> clients;
	private boolean connected = false;
	private String metadata = "";
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
			connected = false;
		} catch (IOException ignored) {}
	}

	public String signup(String uname, String pwd) throws Exception {
		writeOutputStream(new TextMessage("signup " + uname + " " + pwd));
		return readInputStream().getTextMessage();
	}

	public String login(String uname, String pwd) throws Exception {
		writeOutputStream(new TextMessage("login " + uname + " " + pwd));
		return readInputStream().getTextMessage();
	}

	boolean krSuccess(String key, String krFrom, String krTo) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.update(key.getBytes());
			String keyHash = Hex.encodeHexString(messageDigest.digest());
			return (krFrom.equals(krTo) || (krFrom.compareTo(krTo) < 0 && keyHash.compareTo(krFrom) > 0 && keyHash.compareTo(krTo) <= 0)
				|| (krFrom.compareTo(krTo) > 0 && (keyHash.compareTo(krFrom) > 0 || keyHash.compareTo(krTo) <= 0))) ? true : false;
		} catch (NoSuchAlgorithmException ignored) {
			return false;
		}
	}

	private void updateConnection(String key) {
		try {
			disconnect();
			for (String data : metadata.split(";")) {
				if (krSuccess(key, data.split(",")[0], data.split(",")[1])) {
					serverAddr = data.split(",")[2].split(":")[0];
					serverPort = Integer.parseInt(data.split(",")[2].split(":")[1]);
					connect();
					break;
				}
			}
		} catch (IOException | NumberFormatException ignored) {}
	}

	@Override
	public Message put(String key, String value) throws Exception {
		writeOutputStream(new TextMessage("put " + key + " " + value));
		String[] token = readInputStream().getTextMessage().split("\\s+");
		if (token[0].equals("SERVER_STOPPED") || token[0].equals("SERVER_WRITE_LOCK"))
			return new Message(StatusType.valueOf(token[0]), "", "");
		if (token[0].equals("SERVER_NOT_RESPONSIBLE")) {
			metadata = token[1];
			updateConnection(key);
			return put(key, value);
		}
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
		if (token[0].equals("SERVER_STOPPED"))
			return new Message(StatusType.valueOf(token[0]), "", "");
		if (token[0].equals("SERVER_NOT_RESPONSIBLE")) {
			metadata = token[1];
			updateConnection(key);
			return get(key);
		}
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

	public Message keyrange() throws Exception {
		writeOutputStream(new TextMessage("keyrange"));
		String[] token = readInputStream().getTextMessage().split("\\s+");
		metadata = token[1];
		return new Message(StatusType.valueOf(token[0]), "", token[1]);
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
