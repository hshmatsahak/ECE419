package app_kvClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import logger.LogSetup;
import client.KVCommInterface;
import client.KVStore;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

public class KVClient implements IKVClient {

    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private boolean quit = false;
    private BufferedReader stdin;
    private String serverAddr;
    private int serverPort;
    private KVStore kvStore;
    private String uname = "";

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient kvClient = new KVClient();
            kvClient.run();
        } catch (IOException e) {
            perror(PROMPT + "LogSetup");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void run() {
        while (!quit) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmd = stdin.readLine();
                handleCmd(cmd);
            } catch (IOException e) {
                quit = true;
                perror("Quit");
            }
        }
    }

    private void handleCmd(String cmd) {
        String[] token = cmd.split("\\s+");
        switch (token[0]) {
        case "connect":
            if (token.length != 3) {
                perror("Invalid Argument Count");
                break;
            }
            try {
                serverAddr = token[1];
                serverPort = Integer.parseInt(token[2]);
                newConnection(serverAddr, serverPort);
            } catch (NumberFormatException nfe) {
                perror("Invalid Server Port");
            } catch (UnknownHostException e) {
                perror("Invalid Server Address");
                disconnect();
            } catch (IOException e) {
                perror("Connection Establishment Failed");
                disconnect();
            }
            break;
        case "disconnect":
            if (token.length != 1)
                perror("Invalid Argument Count");
            else if (kvStore == null)
                System.out.println(PROMPT + "No Existing Connection");
            else {
                System.out.print(PROMPT + "Disconnecting... ");
                disconnect();
                System.out.println("Done!");
            }
            break;
        case "signup":
            if (token.length != 3) {
                perror("Invalid Argument Count");
                System.out.println(PROMPT + "signup <uname> <pwd>");
            } else if (kvStore == null || !kvStore.isConnected()) {
                perror("Invalid Connection");
                System.out.println(PROMPT + "connect <addr> <port>");
            } else {
                System.out.println(PROMPT + "Signing Up...");
                try {
                    if (kvStore.signup(token[1], token[2]).equals("success")) {
                        uname = token[1];
                        System.out.println(PROMPT + "Done!");
                    } else
                        System.out.println(PROMPT + "Invalid <uname>!");
                } catch (Exception ignored) {}
            }
            break;
        case "login":
            if (token.length != 3) {
                perror("Invalid Argument Count");
                System.out.println(PROMPT + "login <uname> <pwd>");
            } else if (kvStore == null || !kvStore.isConnected()) {
                perror("Invalid Connection");
                System.out.println(PROMPT + "connect <addr> <port>");
            } else {
                if (!uname.isEmpty())
                    System.out.println(PROMPT + "Logging Out Existining User...\n" + PROMPT + "Done!");
                System.out.println(PROMPT + "Logging In...");
                try {
                    if (kvStore.login(token[1], token[2]).equals("success")) {
                        uname = token[1];
                        System.out.println(PROMPT + "Done!");
                    } else
                        System.out.println(PROMPT + "Invalid <uname> or <pwd>!");
                } catch (Exception ignored) {}
            }
            break;
        case "logout":
            if (token.length != 1)
                perror("Too Many Arguments");
            else {
                System.out.println(PROMPT + "Logging Out...");
                if (uname.isEmpty())
                    System.out.println(PROMPT + "No Existing Login");
                else {
                    uname = "";
                    System.out.println(PROMPT + "Done!");
                }
            }
            break;
        case "put":
            if (token.length == 1) {
                perror("Invalid Argument Count");
                System.out.println(PROMPT + "put <key> <val>");
            } else if (kvStore == null || !kvStore.isConnected()) {
                perror("Invalid Connection");
                System.out.println(PROMPT + "connect <addr> <port>");
            } else {
                String key = token[1];
                if (key.length() > 20) {
                    perror("Key Length > 20 Bytes");
                    break;
                }
                if (token.length == 2) {
                    System.out.println("Deleting <key>: " + key + " ...");
                    try {
                        Message kvMessage = kvStore.put(key, "null");
                        if (kvMessage.getStatus() == StatusType.DELETE_ERROR)
                            perror("delete Error");
                        else if (kvMessage.getStatus() == StatusType.SERVER_STOPPED)
                            perror("Storage System Currently Offline... Please Try Again Later");
                        else if (kvMessage.getStatus() == StatusType.SERVER_WRITE_LOCK)
                            perror("Storage System Reconfiguring... Please Try Again");    
                        else {
                            System.out.println("delete Status: " + kvMessage.getStatus());
                            System.out.println("Deleted <key>: " + kvMessage.getKey());
                        }
                    } catch (Exception e) {
                        perror("delete Failed");
                        disconnect();
                    }
                } else {
                    StringBuilder val = new StringBuilder();
                    for (int i=2; i<token.length; ++i) {
                        val.append(token[i]);
                        if (i != token.length - 1)
                            val.append(" ");
                    }
                    System.out.println("Inserting...");
                    try {
                        Message kvMessage = kvStore.put(key, val.toString());
                        if (kvMessage.getStatus() == StatusType.PUT_ERROR)
                            perror("put Error");
                        else if (kvMessage.getStatus() == StatusType.SERVER_STOPPED)
                            perror("Storage System Currently Offline... Please Try Again Later");
                        else if (kvMessage.getStatus() == StatusType.SERVER_WRITE_LOCK)
                            perror("Storage System Reconfiguring... Please Try Again");
                        else {
                            System.out.println("put Status: " + kvMessage.getStatus());
                            System.out.println("Inserted <key>: " + kvMessage.getKey());
                            System.out.println("Corresponding <value>: " + kvMessage.getValue());
                        }
                    } catch (Exception e) {
                        perror("put Failed");
                        disconnect();
                    }
                }
            }
            break;
        case "get":
            if (token.length != 2) {
                perror("Invalid Argument Count");
                System.out.println(PROMPT + "get <key>");
            } else if (kvStore == null || !kvStore.isConnected()) {
                perror("Invalid Connection");
                System.out.println(PROMPT + "connect <addr> <port>");
            } else {
                System.out.println("Retrieving <key>: " + token[1] + " ...");
                try {
                    Message kvMessage = kvStore.get(token[1]);
                    if (kvMessage.getStatus() == StatusType.GET_ERROR)
                        System.out.println("key " + kvMessage.getKey() + " Does Not Exist!");
                    else if (kvMessage.getStatus() == StatusType.SERVER_STOPPED)
                        perror("Storage System Currently Offline... Please Try Again Later");
                    else {
                        System.out.println("Corresponding <value>: " + kvMessage.getValue());
                        System.out.println("get Status: " + kvMessage.getStatus());
                    }
                } catch (Exception e) {
                    perror("get Failed");
                    disconnect();
                }
            }
            break;
        case "keyrange":
            if (token.length != 1)
                perror("Too Many Arguments");
            else if (kvStore == null || !kvStore.isConnected()) {
                perror("Invalid Connection");
                System.out.println(PROMPT + "connect <addr> <port>");
            } else {
                System.out.println("Retrieving keyrange...");
                try {
                    Message kvMessage = kvStore.keyrange();
                    System.out.println(kvMessage.getValue());
                } catch (Exception e) {
                    perror("keyrange Failed");
                    disconnect();
                }
            }
            break;
        case "logLevel":
            if (token.length != 2) {
                perror("Invalid Argument Count");
                break;
            }
            if (token[1].equals(Level.ALL.toString())) {
                logger.setLevel(Level.ALL);
                System.out.println(PROMPT + "Log Level: " + Level.ALL.toString());
            } else if (token[1].equals(Level.DEBUG.toString())) {
                logger.setLevel(Level.DEBUG);
                System.out.println(PROMPT + "Log Level: " + Level.DEBUG.toString());
            } else if (token[1].equals(Level.INFO.toString())) {
                logger.setLevel(Level.INFO);
                System.out.println(PROMPT + "Log Level: " + Level.INFO.toString());
            } else if (token[1].equals(Level.WARN.toString())) {
                logger.setLevel(Level.WARN);
                System.out.println(PROMPT + "Log Level: " + Level.WARN.toString());
            } else if (token[1].equals(Level.ERROR.toString())) {
                logger.setLevel(Level.ERROR);
                System.out.println(PROMPT + "Log Level: " + Level.ERROR.toString());
            } else if (token[1].equals(Level.FATAL.toString())) {
                logger.setLevel(Level.FATAL);
                System.out.println(PROMPT + "Log Level: " + Level.FATAL.toString());
            } else if (token[1].equals(Level.OFF.toString())) {
                logger.setLevel(Level.OFF);
                System.out.println(PROMPT + "Log Level: " + Level.OFF.toString());
            } else {
                perror("Unknown Log Level");
                System.out.println(PROMPT + "Available Log Levels:");
                System.out.println(PROMPT + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
            }
            break;
        case "help":
            System.out.println("\n     Connect To Server: connect <addr> <port>");
            System.out.println("Disconnect From Server: disconnect");
            System.out.println("    Inserting/Updating: put <key> <val>");
            System.out.println("              Deleting: put <key>");
            System.out.println("        Retrieving Key: get <key>");
            System.out.println("             Log Level: logLevel <level>");
            System.out.println("                  Quit: quit\n");
            break;
        case "quit":
            if (token.length != 1) {
                perror("Invalid Argument Count");
                break;
            }
            quit = true;
            disconnect();
            System.out.println(PROMPT + "Exit");
            break;
        default:
            perror("Invalid Command");
            break;
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException {
        if (kvStore != null) {
            kvStore.disconnect();
            System.out.println(PROMPT + "Disconnected Existing Host...");
        }
        kvStore = new KVStore(hostname, port);
        System.out.println(PROMPT + "Establishing Connection...");
        kvStore.connect();
        System.out.println(PROMPT + "Connection Established!");
        kvStore.addClient(this);
//        try {
//            System.out.println(PROMPT + "Establishing Connection...");
//            kvStore.connect();
//            System.out.println(PROMPT + "Connection Established!");
//        } catch (Exception e) {
//            perror("Connection Establishment Failed");
//        }
    }
    
    private void disconnect() {
        if (kvStore != null) {
            kvStore.disconnect();
            kvStore = null;
        }
    }

    @Override
    public KVCommInterface getStore() {
        return kvStore;
    }

    private static void perror(String str) {
        System.out.println(PROMPT + "Error: " + str + "!");
    }
}
