package app_kvServer;

import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;

import shared.messages.TextMessage;

class ServerHeartbeat implements Runnable {

    Socket sock;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    ServerHeartbeat(Socket s) {
        sock = s;
    }

    public void run() {
		try {
			while (true) {
				readInputStream();
				writeOutputStream("heartbeat");
			}
		} catch (IOException ignored) {}
    }

    private void writeOutputStream(String msg) throws IOException {
        byte[] byteMsg = new TextMessage(msg).getByteMessage();
        sock.getOutputStream().write(byteMsg, 0, byteMsg.length);
        sock.getOutputStream().flush();
    }

    private String readInputStream() throws IOException {
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