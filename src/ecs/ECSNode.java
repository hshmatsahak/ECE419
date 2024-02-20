package ecs;

import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;

public class ECSNode implements IECSNode {

    private Socket nodeSock;
    private String nodeName;
    private String nodeAddr;
    private int nodePort;
    private String[] nodeHashRange;

    public ECSNode(Socket sock) {
        nodeSock = sock;
        nodeAddr = sock.getInetAddress().getHostAddress();
        nodePort = sock.getPort();
        nodeName = nodeAddr + ":" + nodePort;
        nodeHashRange = new String[2];
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(nodeName.getBytes());
            nodeHashRange[1] = Hex.encodeHexString(messageDigest.digest());
        } catch (NoSuchAlgorithmException ignored) {}
    }

    public Socket getNodeSock() {
        return nodeSock;
    }

    @Override
    public String getNodeName() {
        return nodeName;
    }

    @Override
    public String getNodeHost() {
        return nodeAddr;
    }

    @Override
    public int getNodePort() {
        return nodePort;
    }

    @Override
    public String[] getNodeHashRange() {
        return nodeHashRange;
    }

    public void setPredecessorHash(String hash) {
        nodeHashRange[0] = hash;
    }
}