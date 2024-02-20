package app_kvECS;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.net.BindException;

import ecs.ECSNode;

class ServerConnection implements Runnable {

    private ECSClient ecsClient;
    private ServerSocket ecsServerSocket;

    ServerConnection(ECSClient client) {
        ecsClient = client;
    }

    public void run() {
        boolean online = initServerConnection();
        while (online) {
            try {
                Socket serverSocket = ecsServerSocket.accept();
                ecsClient.insertNode(new ECSNode(serverSocket));
            } catch (IOException ioe) {
                online = false;
            }
        }
    }

    private boolean initServerConnection() {
        System.out.print("ServerConnection> Initializing... ");
        try {
            ecsServerSocket = new ServerSocket(ecsClient.ecsPort);
            System.out.println("Online! Port " + ecsClient.ecsPort);
            return true;
        } catch (IOException ioe) {
            System.out.println("Error: Socket Bind Failed!");
            if (ioe instanceof BindException)
                System.out.println("ServerConnection> Port " + ecsClient.ecsPort + " Unavailable!");
            return false;
        }
    }
}