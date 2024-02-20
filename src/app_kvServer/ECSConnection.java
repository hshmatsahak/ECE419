package app_kvServer;

import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import shared.messages.TextMessage;

class ECSConnection implements Runnable {

    private KVServer kvServer;
    private Socket ecsSock;
    private InputStream inputStream;
    private OutputStream outputStream;
    private int serverPort;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    ECSConnection(KVServer server, Socket sock, int port) throws IOException {
        kvServer = server;
        ecsSock = sock;
        inputStream = sock.getInputStream();
        outputStream = sock.getOutputStream();
        serverPort = port;
    }

    public void run() {
        try {
            writeOutputStream(new TextMessage(Integer.toString(serverPort)));
        } catch (IOException ignored) {}
        while (true) {}
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

    private void writeOutputStream(TextMessage msg) throws IOException {
        byte[] byteMsg = msg.getByteMessage();
        outputStream.write(byteMsg, 0, byteMsg.length);
        outputStream.flush();
    }
}