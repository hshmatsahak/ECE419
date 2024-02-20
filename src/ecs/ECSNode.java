package ecs;

import java.net.Socket;

public class ECSNode implements IECSNode {

    private Socket serverSock;
    private String serverName;
    private String serverAddr;
    private int serverPort;
    private String[] serverHashRange;

    public ECSNode(Socket sock) {
        serverSock = sock;
        serverAddr = sock.getInetAddress().getHostAddress();
        serverPort = sock.getPort();
        serverName = serverAddr + ":" + serverPort;
    }

    public Socket getNodeSock() {
        return serverSock;
    }

    @Override
    public String getNodeName() {
        return serverName;
    }

    @Override
    public String getNodeHost() {
        return serverAddr;
    }

    @Override
    public int getNodePort() {
        return serverPort;
    }

    @Override
    public String[] getNodeHashRange() {
        return serverHashRange;
    }
}