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
import shared.messages.KVMessage;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private boolean quit = false;
    private BufferedReader stdin;
    private String serverAddr;
    private int serverPort;
    private KVStore kvStore = null;

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient kvClient = new KVClient();
            kvClient.run();
        } catch (IOException e) {
            System.out.println(PROMPT + "Error: LogSetup!");
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
            } catch (Exception e) {
                perror("Connection Establishment Failed");
            }
            break;
        case "disconnect":
            if (token.length != 1) {
                perror("Invalid Argument Count");
                break;
            }
            disconnect();
            break;
        case "put":
            if (token.length == 1) {
                perror("Invalid Argument Count");
                System.out.println(PROMPT + "put <key> <val>");
            } else if (kvStore == null) {
                perror("Invalid Connection");
                System.out.println(PROMPT + "connect <addr> <port>");
            } else {
                String key = token[1];
                if (key.length() > 20) {
                    perror("Key Length > 20 Bytes");
                    break;
                }
                String val;
                if (token.length == 2)
                    val = null;
                else {
                    StringBuilder sb = new StringBuilder();
                    for (int i=2; i<token.length; ++i) {
                        sb.append(token[i]);
                        if (i != token.length - 1)
                            sb.append(" ");
                    }
                    val = sb.toString();
                }
                try {
                    KVMessage kvMessage = kvStore.put(key, val);
                    if (kvMessage.getStatus().equals(KVMessage.StatusType.PUT_ERROR))
                        perror("put Error");
                    else {
                        System.out.println("put <key>: " + kvMessage.getKey());
                        System.out.println("Corresponding <value>: " + kvMessage.getValue());
                        System.out.println("Status: " + kvMessage.getStatus());
                    }
                } catch (Exception e) {
                    perror("put Failed");
                    disconnect();
                }
            }
            break;
        default:
            perror("Invalid Command");
            break;
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws IOException, UnknownHostException {
        if (kvStore != null) {
            kvStore.disconnect();
            System.out.println(PROMPT + "Disconnected Existing Host");
        }
        kvStore = new KVStore(hostname, port);
        try {
            System.out.println(PROMPT + "Establishing Connection...");
            kvStore.connect();
            System.out.println(PROMPT + "Connection Established!");
        } catch (Exception e) {
            perror("Connection Establishment Failed");
        }
    }
    
    private void disconnect() {
        if (kvStore != null) {
            kvStore.disconnect();
            kvStore = null;
        }
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }

    private void perror(String str) {
        System.out.println(PROMPT + "Error: " + str + "!");
    }
}
