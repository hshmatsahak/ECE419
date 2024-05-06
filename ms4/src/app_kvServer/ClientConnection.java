package app_kvServer;

import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

//import shared.messages.Message;
//import shared.messages.KVMessage.StatusType;
import shared.messages.TextMessage;

class ClientConnection implements Runnable {

    private static final Logger logger = Logger.getRootLogger();
    private final Socket clientSocket;
    private String ecsAddr;
    private int ecsPort;
    private KVServer clientServer;
    private InputStream inputStream;
    private OutputStream outputStream;
//    private ObjectInputStream objectInputStream;
//    private ObjectOutputStream objectOutputStream;
    private boolean connected = true;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    public ClientConnection(Socket client, String addr, int port) {
        clientSocket = client;
        ecsAddr = addr;
        ecsPort = port;
    }

    public void addServer(KVServer server) {
        clientServer = server;
    }

    public void run() {
        try {
            inputStream = clientSocket.getInputStream();
            outputStream = clientSocket.getOutputStream();
//            objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
//            objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            logger.info("Connecting... Done!");
            while (connected) {
                try {
                    TextMessage clientTextMessage = readInputStream();
                    clientServer.serverLock.lock();
                    TextMessage serverTextMessage = handleTextMsg(clientTextMessage);
                    clientServer.serverLock.unlock();
                    writeOutputStream(serverTextMessage);
                } catch (IOException ioe) {
                    connected = false;
                    logger.error("Client Disconnected!");
                }
//                try {
//                    Message clientMessage = (Message) objectInputStream.readObject();
//                    clientServer.serverLock.lock();
//                    Message serverMessage = handleMsg(clientMessage);
//                    clientServer.serverLock.unlock();
//                    if (serverMessage == null) {
//                        connected = false;
//                    } else {
//                        objectOutputStream.writeObject(serverMessage);
//                    }
//                } catch (IOException ioe) {
//                    connected = false;
////                    logger.error("Error: Connection Failed!");
//                } catch (ClassNotFoundException e) {
//                    connected = false;
////                    logger.error("Error: Class Not Found!");
//                }
            }
        } catch (IOException ioe) {
            logger.error("Connecting... Error!");
        } finally {
            try {
//                if (objectInputStream != null)
//                    objectInputStream.close();
//                if (objectOutputStream != null)
//                    objectOutputStream.close();
                if (clientSocket != null) {
                    clientSocket.close();
                    logger.info("Socket Closed!");
                }
            } catch (IOException ioe) {
                logger.error("Error Closing Client Socket!");
            }
        }
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

    private TextMessage readInputStream(Socket sock) throws IOException {
        byte read = (byte) sock.getInputStream().read();
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
            read = (byte) sock.getInputStream().read();
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

    private TextMessage handleTextMsg(TextMessage msg) {
        String[] token = msg.getTextMessage().split("\\s+");
        if (token[0].equals("signup") || token[0].equals("login")) {
            try {
                Socket ecsSock = new Socket(ecsAddr, ecsPort);
                writeOutputStream(ecsSock, msg);
                return readInputStream(ecsSock);
            } catch (IOException ignored) {
                return new TextMessage("failed");
            }
        } else if (token[0].equals("put") && token.length > 3) {
            if (clientServer.stopped)
                return new TextMessage("SERVER_STOPPED");
            if (clientServer.write_lock)
                return new TextMessage("SERVER_WRITE_LOCK");
            if (!clientServer.krSuccess(token[2].toString()))
                return new TextMessage("SERVER_NOT_RESPONSIBLE " + clientServer.metadata);
            StringBuilder val = new StringBuilder();
            for (int i=3; i<token.length; ++i) {
                val.append(token[i]);
                if (i != token.length - 1)
                    val.append(" ");
            }
            boolean isUpdate = clientServer.inStorage(token[2]);
            try {
                clientServer.put(token[1], token[2], val.toString());
                if (val.toString().equals("null")) {
                    return new TextMessage("DELETE_SUCCESS " + token[2]);
                } else if (isUpdate) {
                    return new TextMessage("PUT_UPDATE " + token[2] + " " + val);
                } else {
                    return new TextMessage("PUT_SUCCESS " + token[2] + " " + val);
                }
            } catch (Exception e) {
                if (val.toString().equals("null")) {
                    return new TextMessage("DELETE_ERROR " + token[2]);
                } else {
                    return new TextMessage("PUT_ERROR " + token[2] + " " + val);
                }
            }
        } else if (token[0].equals("put_replica_1") || token[0].equals("put_replica_2")) {
            if (clientServer.stopped)
                return new TextMessage("SERVER_STOPPED");
            if (clientServer.write_lock)
                return new TextMessage("SERVER_WRITE_LOCK");
            try {
                StringBuilder val = new StringBuilder();
                for (int i=2; i<token.length; ++i) {
                    val.append(token[i]);
                    if (i != token.length - 1)
                        val.append(" ");
                }
                clientServer.putKV(token[1], val.toString(), token[0].equals("put_replica_1") ? "replica_1" : "replica_2");
                return new TextMessage("success");
            } catch (Exception ignored) {
                return new TextMessage("failed");
            }
        } else if (token[0].equals("get") && token.length == 3) {
            if (clientServer.stopped)
                return new TextMessage("SERVER_STOPPED");
            if (!clientServer.krSuccess(token[2].toString()))
                return new TextMessage("SERVER_NOT_RESPONSIBLE " + clientServer.metadata);
            try {
                String val = clientServer.get(token[1], token[2]);
                return new TextMessage("GET_SUCCESS " + token[2] + " " + val);
            } catch (Exception e) {
                return new TextMessage("GET_ERROR " + token[2]);
            }
        } else if (token[0].equals("keyrange") && token.length == 1) {
            return new TextMessage("KEYRANGE_SUCCESS " + clientServer.metadata);
        } else if (token[0].equals("transfer") && token.length > 2) {
            try {
                StringBuilder val = new StringBuilder();
                for (int i=2; i<token.length; ++i) {
                    val.append(token[i]);
                    if (i != token.length - 1)
                        val.append(" ");
                }
                clientServer.putKV(token[1], val.toString());
                return new TextMessage("success");
            } catch (Exception ignored) {
                return new TextMessage("failed");
            }
        } else if (token[0].equals("transfer_replica_1") || token[0].equals("transfer_replica_2")) {
            if (token.length == 1) {
                clientServer.cleanReplicaDirectory(token[0].equals("transfer_replica_1") ? 1 : 2);
                return new TextMessage("success");
            }
            try {
                StringBuilder val = new StringBuilder();
                for (int i=2; i<token.length; ++i) {
                    val.append(token[i]);
                    if (i != token.length - 1)
                        val.append(" ");
                }
                clientServer.putKV(token[1], val.toString(), token[0].equals("transfer_replica_1") ? "replica_1" : "replica_2");
                return new TextMessage("success");
            } catch (Exception ignored) {
                return new TextMessage("failed");
            }
        } else {
            return new TextMessage("FAILED");
        }
    }

    private void writeOutputStream(TextMessage msg) throws IOException {
        byte[] byteMsg = msg.getByteMessage();
        outputStream.write(byteMsg, 0, byteMsg.length);
        outputStream.flush();
    }

    private void writeOutputStream(Socket sock, TextMessage msg) throws IOException {
        byte[] byteMsg = msg.getByteMessage();
        sock.getOutputStream().write(byteMsg, 0, byteMsg.length);
        sock.getOutputStream().flush();
    }

//    private Message handleMsg(Message msg) {
//        StatusType type = msg.getStatus();
//        String key = msg.getKey();
//        String val = msg.getValue();
//        if (!(type == StatusType.PUT && !key.isBlank() && !val.isBlank())
//                && !(type == StatusType.GET && !key.isBlank() && val.isBlank())) {
//            return null;
//        } else if (type == StatusType.PUT) {
//            boolean isUpdate = clientServer.inStorage(key);
//            try {
//                clientServer.putKV(key, val);
//                if (val.equals("null")) {
//                    type = StatusType.DELETE_SUCCESS;
//                } else if (isUpdate) {
//                    type = StatusType.PUT_UPDATE;
//                } else {
//                    type = StatusType.PUT_SUCCESS;
//                }
//            } catch (Exception e) {
//                if (val.equals("null")) {
//                    type = StatusType.DELETE_ERROR;
//                } else {
//                    type = StatusType.PUT_ERROR;
//                }
//                val = e.getMessage();
//            }
//        } else {
//            try {
//                val = clientServer.getKV(key);
//                type = StatusType.GET_SUCCESS;
//            } catch (Exception e) {
//                type = StatusType.GET_ERROR;
//                val = e.getMessage();
//            }
//        }
//        return new Message(type, key, val);
//    }
}
