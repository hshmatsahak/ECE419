package app_kvServer;

import java.net.Socket;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

public class ClientConnection implements Runnable {

    private static final Logger logger = Logger.getRootLogger();
    private final Socket clientSocket;
    private KVServer clientServer;
    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;
    private boolean connected = true;

    public ClientConnection(Socket client) {
        clientSocket = client;
    }

    public void addServer(KVServer server) {
        clientServer = server;
    }

    public void run() {
        try {
            objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
            objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            while (connected) {
                try {
                    Message clientMessage = (Message) objectInputStream.readObject();
                    clientServer.serverLock.lock();
                    Message serverMessage = handleMsg(clientMessage);
                    clientServer.serverLock.unlock();
                    if (serverMessage == null) {
                        connected = false;
                    } else {
                        objectOutputStream.writeObject(serverMessage);
                    }
                } catch (IOException ioe) {
                    connected = false;
//                    logger.error("Error: Connection Failed!");
                } catch (ClassNotFoundException e) {
                    connected = false;
//                    logger.error("Error: Class Not Found!");
                }
            }
        } catch (IOException ioe) {
//            logger.error("Error: Connecting... Failed!");
        } finally {
            try {
                if (objectInputStream != null)
                    objectInputStream.close();
                if (objectOutputStream != null)
                    objectOutputStream.close();
                if (clientSocket != null)
                    clientSocket.close();
            } catch (IOException ioe) {
//                logger.error("Error: Disconnecting... Failed!");
            }
        }
    }

    private Message handleMsg(Message msg) {
        StatusType type = msg.getStatus();
        String key = msg.getKey();
        String val = msg.getValue();
        if (!(type == StatusType.PUT && key != null && !val.isEmpty())
                && !(type == StatusType.GET && key != null && val.isEmpty())) {
            return null;
        } else if (type == StatusType.PUT) {
            boolean update = clientServer.inStorage(key);
            try {
                clientServer.putKV(key, val);
                if (val.equals("null")) {
                    type = StatusType.DELETE_SUCCESS;
                } else if (update) {
                    type = StatusType.PUT_UPDATE;
                } else {
                    type = StatusType.PUT_SUCCESS;
                }
            } catch (Exception e) {
                if (val.equals("null")) {
                    type = StatusType.DELETE_ERROR;
                } else {
                    type = StatusType.PUT_ERROR;
                }
            }
        } else {
            try {
                val = clientServer.getKV(key);
                type = StatusType.GET_SUCCESS;
            } catch (Exception e) {
                type = StatusType.GET_ERROR;
            }
        }
        return new Message(type, key, val);
    }
}