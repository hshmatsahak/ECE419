package app_kvECS;

import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import shared.messages.TextMessage;

class ECSHeartbeat implements Runnable {

    ECSClient ecs;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    ECSHeartbeat(ECSClient client) {
        ecs = client;
    }

    public void run() {
        while (true) {
            synchronized (ecs.heartbeat) {
                for (Map.Entry<String, Socket> entry : ecs.heartbeat.entrySet()) {
                    try {
                        writeOutputStream(entry.getValue(), "heartbeat");
                        readInputStream(entry.getValue());
                    } catch (IOException ioe) {
                        try {
                            ecs.shutdownNode(entry.getKey().split(":")[0], Integer.parseInt(entry.getKey().split(":")[1]));
                        } catch (NumberFormatException ignored) {}
                        ecs.heartbeat.remove(entry.getKey(), entry.getValue());
                        break;
                    }
                }
            }
        }
    }

    private void writeOutputStream(Socket sock, String msg) throws IOException {
        byte[] byteMsg = new TextMessage(msg).getByteMessage();
        sock.getOutputStream().write(byteMsg, 0, byteMsg.length);
        sock.getOutputStream().flush();
    }

    private String readInputStream(Socket sock) throws IOException {
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
        return new TextMessage(byteMessage).getTextMessage();
    }
}