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
    private boolean connected = true;
    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;

    public ClientConnection(Socket client) {
        clientSocket = client;
    }

    public void run() {
        try {
            objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
            objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            while (connected) {
                try {
                    Message clientMessage = (Message) objectInputStream.readObject();
                    objectOutputStream.writeObject(clientMessage);
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
                if (clientSocket != null) {
                    objectInputStream.close();
                    objectOutputStream.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
//                logger.error("Error: Disconnecting... Failed!");
            }
        }
    }
}