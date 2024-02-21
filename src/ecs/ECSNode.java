package ecs;

import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import shared.messages.TextMessage;

public class ECSNode implements IECSNode {

    private Socket nodeSock;
    private String nodeName;
    private String nodeAddr;
    private int nodePort;
    private int serverPort;
    private String[] nodeHashRange;
    private InputStream inputStream;
    private OutputStream outputStream;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    public ECSNode(Socket sock, int port) {
        nodeSock = sock;
        nodeAddr = sock.getInetAddress().getHostAddress();
        nodePort = sock.getPort();
        nodeName = nodeAddr + ":" + nodePort;
        serverPort = port;
        nodeHashRange = new String[2];
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(nodeName.getBytes());
            nodeHashRange[1] = Hex.encodeHexString(messageDigest.digest());
            inputStream = sock.getInputStream();
            outputStream = sock.getOutputStream();
        } catch (NoSuchAlgorithmException | IOException ignored) {}
    }

    public Socket getNodeSock() {
        return nodeSock;
    }

    public int getServerPort() {
        return serverPort;
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

    public TextMessage readInputStream() throws IOException {
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

    public void writeOutputStream(TextMessage msg) throws IOException {
        byte[] byteMsg = msg.getByteMessage();
        outputStream.write(byteMsg, 0, byteMsg.length);
        outputStream.flush();
    }
}