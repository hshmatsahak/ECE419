package app_kvServer;

import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.io.File;
import java.util.Scanner;
import java.util.HashSet;

import shared.messages.TextMessage;

class ECSConnection implements Runnable {

    private KVServer kvServer;
    private Socket ecsSock;
    private InputStream inputStream;
    private OutputStream outputStream;
    private int serverPort;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private ArrayList<File> transferFile = new ArrayList<>();
    private String metadata = "";
    private String[] keyRange = {"", ""};

    ECSConnection(KVServer server, Socket sock, int port) throws IOException {
        kvServer = server;
        ecsSock = sock;
        inputStream = sock.getInputStream();
        outputStream = sock.getOutputStream();
        serverPort = port;
    }

    public void run() {
        try {
            writeOutputStream(new TextMessage(Integer.toString(serverPort)), outputStream);
        } catch (IOException ignored) {}
        while (true) {
            try {
                TextMessage ecsMsg = readInputStream(inputStream);
                TextMessage serverMsg = handleMsg(ecsMsg);
                writeOutputStream(serverMsg, outputStream);
            } catch (IOException ignored) {}
        }
    }

    private TextMessage handleMsg(TextMessage msg) {
        String[] token = msg.getTextMessage().split("\\s+");
        switch (token[0]) {
        case "add":
            for (String newMetadata : token[1].split(";")) {
                String[] data = newMetadata.split(",");
                if (data[2].equals(ecsSock.getInetAddress().getHostAddress() + ":" + serverPort)) {
                    if (!kvServer.keyRange[0].isEmpty() && !kvServer.keyRange[0].equals(data[0])) {
                        kvServer.write_lock = true;
                        try {
                            Socket interServerConnection = new Socket(token[2].split(":")[0], Integer.parseInt(token[2].split(":")[1]));
                            OutputStream serverOutput = interServerConnection.getOutputStream();
                            InputStream serverInput = interServerConnection.getInputStream();
                            transferFile = kvServer.transferKeyRange(kvServer.keyRange[0], data[0]);
                            for (File file : transferFile) {
                                Scanner fileScanner = new Scanner(file);
                                writeOutputStream(new TextMessage("transfer " + file.getName() + " " + fileScanner.nextLine()), serverOutput);
                                fileScanner.close();
                                readInputStream(serverInput);
                            }
                            interServerConnection.close();
                        } catch (NumberFormatException | IOException ignored) {}
                    }
                    metadata = token[1];
                    keyRange[0] = data[0];
                    keyRange[1] = data[1];
                    return new TextMessage("success");
                }
            }
            break;
        case "add_success":
            transferFile.forEach(file -> file.delete());
            transferFile.clear();
            kvServer.metadata = metadata;
            metadata = "";
            kvServer.keyRange[0] = keyRange[0];
            keyRange[0] = "";
            kvServer.keyRange[1] = keyRange[1];
            keyRange[1] = "";
            kvServer.write_lock = false;
            return new TextMessage("success");
        case "remove":
            String predecessorKeyRange = "";
            if (token.length == 3) {
                for (String newMetadata : token[1].split(";"))
                    if (newMetadata.split(",")[2].equals(ecsSock.getInetAddress().getHostAddress() + ":" + serverPort))
                        predecessorKeyRange = newMetadata.split(",")[0];
                if (predecessorKeyRange.isEmpty()) {
                    kvServer.write_lock = true;
                    try {
                        Socket interServerConnection = new Socket(token[2].split(":")[0], Integer.parseInt(token[2].split(":")[1]));
                        OutputStream serverOutput = interServerConnection.getOutputStream();
                        InputStream serverInput = interServerConnection.getInputStream();
                        transferFile = kvServer.transferKeyRange(kvServer.keyRange[0], kvServer.keyRange[1]);
                        for (File file : transferFile) {
                            Scanner fileScanner = new Scanner(file);
                            writeOutputStream(new TextMessage("transfer " + file.getName() + " " + fileScanner.nextLine()), serverOutput);
                            fileScanner.close();
                            readInputStream(serverInput);
                        }
                        interServerConnection.close();
                    } catch (NumberFormatException | IOException ignored) {}
                }
                metadata = token[1];
            } else
                metadata = "";
            keyRange[0] = predecessorKeyRange;
            return new TextMessage("success");
        case "remove_success":
            transferFile.forEach(file -> file.delete());
            transferFile.clear();
            kvServer.metadata = metadata;
            metadata = "";
            kvServer.keyRange[0] = keyRange[0];
            keyRange[0] = "";
            kvServer.write_lock = false;
            return new TextMessage("success");
        case "metadata":
            kvServer.metadata = token.length == 2 ? token[1] : "";
            return new TextMessage("success");
        case "start":
            kvServer.stopped = false;
            return new TextMessage("success");
        case "stop":
            kvServer.stopped = true;
            return new TextMessage("success");
        default:
            break;
        }
        return null;
    }

    private TextMessage readInputStream(InputStream stream) throws IOException {
        byte read = (byte) stream.read();
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
            read = (byte) stream.read();
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

    private void writeOutputStream(TextMessage msg, OutputStream stream) throws IOException {
        byte[] byteMsg = msg.getByteMessage();
        stream.write(byteMsg, 0, byteMsg.length);
        stream.flush();
    }
}