package app_kvServer;

import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

class ECSConnection implements Runnable {

    private KVServer kvServer;
    private Socket ecsSock;
    private InputStream inputStream;
    private OutputStream outputStream;

    ECSConnection(KVServer server, Socket sock) throws IOException {
        kvServer = server;
        ecsSock = sock;
        inputStream = sock.getInputStream();
        outputStream = sock.getOutputStream();
    }

    public void run() {
        while (true) {}
    }
}