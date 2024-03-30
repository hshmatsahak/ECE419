package app_kvServer;

import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.List;
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
    private Socket heartbeatSock;

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
            heartbeatSock = new Socket(ecsSock.getInetAddress(), ecsSock.getPort());
            new Thread(new ServerHeartbeat(heartbeatSock)).start();
            writeOutputStream(new TextMessage("heartbeat 127.0.0.1:" + serverPort), heartbeatSock.getOutputStream());
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
                    kvServer.cleanReplicaDirectory(1);
                    kvServer.cleanReplicaDirectory(2);
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
            kvServer.keyRange[0] = keyRange[0];
            keyRange[0] = "";
            kvServer.keyRange[1] = keyRange[1];
            keyRange[1] = "";
            String[] replica = new String[2];
            String[] server = metadata.split(";");
            for (int i = 0; i < server.length; ++i) {
                if (!server[i].split(",")[2].equals("127.0.0.1:" + serverPort)) continue;
                if (server.length > 1) replica[0] = (i+1 == server.length) ? server[0].split(",")[2] : server[i+1].split(",")[2];
                if (server.length <= 2) break;
                if (i+1 == server.length) replica[1] = server[1].split(",")[2];
                else if (i+2 == server.length) replica[1] = server[0].split(",")[2];
                else replica[1] = server[i+2].split(",")[2];
                break;
            }
            try {
                if (server.length == 1) {
                    metadata = "";
                    kvServer.write_lock = false;
                    return new TextMessage("success");
                }
                Socket replicaSock = new Socket(replica[0].split(":")[0], Integer.parseInt(replica[0].split(":")[1]));
                writeOutputStream(new TextMessage("transfer_replica_1"), replicaSock.getOutputStream());
                readInputStream(replicaSock.getInputStream());
                File[] coordinatorFile = kvServer.getCoordinatorFile();
                for (File file : coordinatorFile) {
                    Scanner fileScanner = new Scanner(file);
                    writeOutputStream(new TextMessage("transfer_replica_1 " + file.getName() + " " + fileScanner.nextLine()), replicaSock.getOutputStream());
                    fileScanner.close();
                    readInputStream(replicaSock.getInputStream());
                }
                replicaSock.close();
                if (server.length == 2) {
                    metadata = "";
                    kvServer.write_lock = false;
                    return new TextMessage("success");
                }
                replicaSock = new Socket(replica[1].split(":")[0], Integer.parseInt(replica[1].split(":")[1]));
                writeOutputStream(new TextMessage("transfer_replica_2"), replicaSock.getOutputStream());
                readInputStream(replicaSock.getInputStream());
                for (File file : coordinatorFile) {
                    Scanner fileScanner = new Scanner(file);
                    writeOutputStream(new TextMessage("transfer_replica_2 " + file.getName() + " " + fileScanner.nextLine()), replicaSock.getOutputStream());
                    fileScanner.close();
                    readInputStream(replicaSock.getInputStream());
                }
                replicaSock.close();
            } catch (IOException | NumberFormatException ignored) {}
            metadata = "";
            kvServer.write_lock = false;
            return new TextMessage("success");
        case "remove":
            String predecessorKeyRange = "";
            if (token.length == 3) {
                kvServer.cleanReplicaDirectory(1);
                kvServer.cleanReplicaDirectory(2);
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
            kvServer.keyRange[0] = keyRange[0];
            keyRange[0] = "";
            replica = new String[2];
            server = metadata.split(";");
            for (int i = 0; i < server.length; ++i) {
                if (server[i].isEmpty()) break;
                if (!server[i].split(",")[2].equals("127.0.0.1:" + serverPort)) continue;
                if (server.length > 1) replica[0] = (i+1 == server.length) ? server[0].split(",")[2] : server[i+1].split(",")[2];
                if (server.length <= 2) break;
                if (i+1 == server.length) replica[1] = server[1].split(",")[2];
                else if (i+2 == server.length) replica[1] = server[0].split(",")[2];
                else replica[1] = server[i+2].split(",")[2];
                break;
            }
            try {
                if (server.length == 1) {
                    metadata = "";
                    kvServer.write_lock = false;
                    return new TextMessage("success");
                }
                Socket replicaSock = new Socket(replica[0].split(":")[0], Integer.parseInt(replica[0].split(":")[1]));;
                File[] coordinatorFile = kvServer.getCoordinatorFile();
                for (File file : coordinatorFile) {
                    Scanner fileScanner = new Scanner(file);
                    writeOutputStream(new TextMessage("transfer_replica_1 " + file.getName() + " " + fileScanner.nextLine()), replicaSock.getOutputStream());
                    fileScanner.close();
                    readInputStream(replicaSock.getInputStream());
                }
                replicaSock.close();
                if (server.length == 2) {
                    metadata = "";
                    kvServer.write_lock = false;
                    return new TextMessage("success");
                }
                replicaSock = new Socket(replica[1].split(":")[0], Integer.parseInt(replica[1].split(":")[1]));
                for (File file : coordinatorFile) {
                    Scanner fileScanner = new Scanner(file);
                    writeOutputStream(new TextMessage("transfer_replica_2 " + file.getName() + " " + fileScanner.nextLine()), replicaSock.getOutputStream());
                    fileScanner.close();
                    readInputStream(replicaSock.getInputStream());
                }
                replicaSock.close();
            } catch (IOException | NumberFormatException ignored) {}
            metadata = "";
            kvServer.write_lock = false;
            return new TextMessage("success");
        case "shutdown":
            for (String newMetadata : token[1].split(";")) {
                if (!newMetadata.split(",")[2].equals(ecsSock.getInetAddress().getHostAddress() + ":" + serverPort) || !newMetadata.split(",")[2].equals(token[2]))
                    continue;
                kvServer.keyRange[0] = newMetadata.split(",")[0];
                kvServer.transferReplicaToCoordinator(1);
                break;
            }
            kvServer.metadata = token[1];
            kvServer.cleanReplicaDirectory(1);
            kvServer.cleanReplicaDirectory(2);
            return new TextMessage("success");
        case "shutdown_success":
            replica = new String[2];
            server = metadata.split(";");
            for (int i = 0; i < server.length; ++i) {
                if (server[i].isEmpty()) break;
                if (!server[i].split(",")[2].equals("127.0.0.1:" + serverPort)) continue;
                if (server.length > 1) replica[0] = (i+1 == server.length) ? server[0].split(",")[2] : server[i+1].split(",")[2];
                if (server.length <= 2) break;
                if (i+1 == server.length) replica[1] = server[1].split(",")[2];
                else if (i+2 == server.length) replica[1] = server[0].split(",")[2];
                else replica[1] = server[i+2].split(",")[2];
                break;
            }
            try {
                if (server.length == 1) {
                    metadata = "";
                    kvServer.write_lock = false;
                    return new TextMessage("success");
                }
                Socket replicaSock = new Socket(replica[0].split(":")[0], Integer.parseInt(replica[0].split(":")[1]));;
                File[] coordinatorFile = kvServer.getCoordinatorFile();
                for (File file : coordinatorFile) {
                    Scanner fileScanner = new Scanner(file);
                    writeOutputStream(new TextMessage("transfer_replica_1 " + file.getName() + " " + fileScanner.nextLine()), replicaSock.getOutputStream());
                    fileScanner.close();
                    readInputStream(replicaSock.getInputStream());
                }
                replicaSock.close();
                if (server.length == 2) {
                    metadata = "";
                    kvServer.write_lock = false;
                    return new TextMessage("success");
                }
                replicaSock = new Socket(replica[1].split(":")[0], Integer.parseInt(replica[1].split(":")[1]));
                for (File file : coordinatorFile) {
                    Scanner fileScanner = new Scanner(file);
                    writeOutputStream(new TextMessage("transfer_replica_2 " + file.getName() + " " + fileScanner.nextLine()), replicaSock.getOutputStream());
                    fileScanner.close();
                    readInputStream(replicaSock.getInputStream());
                }
                replicaSock.close();
            } catch (IOException | NumberFormatException ignored) {}
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