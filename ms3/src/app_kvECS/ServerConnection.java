package app_kvECS;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.net.BindException;
import java.io.InputStream;

import ecs.ECSNode;
import shared.messages.TextMessage;

class ServerConnection implements Runnable {

    private ECSClient ecsClient;
    private ServerSocket ecsServerSocket;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    ServerConnection(ECSClient client) {
        ecsClient = client;
    }

    public void run() {
        boolean online = initServerConnection();
        while (online) {
            try {
                Socket serverSocket = ecsServerSocket.accept();
                String[] serverMsg = readInputStream(serverSocket).getTextMessage().split("\\s+");
                if (serverMsg.length != 1) {
                    serverSocket.setSoTimeout(1*1000);
                    synchronized (ecsClient.heartbeat) {ecsClient.heartbeat.put(serverMsg[1], serverSocket);}
                    // ecsClient.shutdownNode(serverSocket.getInetAddress().getHostAddress(), Integer.parseInt(serverMsg[1]));
                    // writeOutputStream(serverSocket, new TextMessage("success"));
                } else
                    ecsClient.insertNode(new ECSNode(serverSocket, Integer.parseInt(serverMsg[0])));
            } catch (NumberFormatException | IOException ignored) {}
        }
    }

    private boolean initServerConnection() {
        System.out.print("ECS> Initializing... ");
        try {
            new Thread(new ECSHeartbeat(ecsClient)).start();
            ecsServerSocket = new ServerSocket(ecsClient.ecsPort);
            System.out.println("Online! Port " + ecsClient.ecsPort);
            return true;
        } catch (IOException ioe) {
            System.out.println("Error: Socket Bind Failed!");
            if (ioe instanceof BindException)
                System.out.println("ECS> Port " + ecsClient.ecsPort + " Unavailable!");
            return false;
        }
    }

    private TextMessage readInputStream(Socket sock) throws IOException {
        InputStream inputStream = sock.getInputStream();
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

    public void writeOutputStream(Socket sock, TextMessage msg) throws IOException {
        byte[] byteMsg = msg.getByteMessage();
        sock.getOutputStream().write(byteMsg, 0, byteMsg.length);
        sock.getOutputStream().flush();
    }
}