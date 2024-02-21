package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import ecs.IECSNode;
import ecs.ECSNode;
import shared.messages.TextMessage;

public class ECSClient implements IECSClient {

    final int ecsPort;
    private Map<String, ECSNode> availableNode;
    private ArrayList<ECSNode> nodeRing;
    private Map<String, ECSNode> occupiedNode;
    private boolean quit = false;

    public static void main(String[] args) {
        if (args.length == 1) {
            try {
                int port = Integer.parseInt(args[0]);
                ECSClient ecsClient = new ECSClient(port);
                Thread serverConnection = new Thread(new ServerConnection(ecsClient));
                serverConnection.start();
                try {
                    Thread.sleep(99);
                } catch (InterruptedException ignored) {}
                ecsClient.run();
            } catch (NumberFormatException nfe) {
                System.out.println("ECS> Error: Invalid ECS Port!");
                System.exit(1);
            }
        } else {
            System.out.println("ECS> Error: Invalid Argument Count!");
            System.exit(1);
        }
    }

    public ECSClient(int port) {
        ecsPort = port;
        availableNode = new HashMap<>();
        occupiedNode = new HashMap<>();
        nodeRing = new ArrayList<>();
    }

    public void insertNode(ECSNode node) {
        availableNode.put(node.getNodeName(), node);
    }

    private void run() {
        while (!quit) {
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("ECS> ");
            try {
                String cmd = stdin.readLine();
                handleCmd(cmd);
            } catch (IOException ioe) {
                System.exit(1);
            }
        }
    }

    private void handleCmd(String cmd) {
        String[] token = cmd.split("\\s+");
        switch (token[0]) {
        case "add":
            try {
                addNode(Integer.parseInt(token[1]));
            } catch (NumberFormatException ignored) {}
            break;
        }
    }

    private void addNode(int count) {
        if (count > availableNode.size())
            System.out.println("ECS> Error: Too Many Nodes!");
        ECSNode node = new ArrayList<>(availableNode.values()).get(0);
        availableNode.remove(node.getNodeName());
        String nodeHash = node.getNodeHashRange()[1];
        if (nodeRing.isEmpty()) {
            node.setPredecessorHash(nodeHash);
            nodeRing.add(node);
        } else if (nodeHash.compareTo(nodeRing.get(nodeRing.size()-1).getNodeHashRange()[1]) > 0
                || nodeHash.compareTo(nodeRing.get(0).getNodeHashRange()[1]) < 0) {
            nodeRing.get(0).setPredecessorHash(nodeHash);
            node.setPredecessorHash(nodeRing.get(nodeRing.size()-1).getNodeHashRange()[1]);
            if (nodeHash.compareTo(nodeRing.get(0).getNodeHashRange()[1]) < 0)
                nodeRing.add(0, node);
            else
                nodeRing.add(node);
        } else {
            for (int i = 1; i < nodeRing.size(); i++) {
                if (nodeHash.compareTo(nodeRing.get(i-1).getNodeHashRange()[1]) > 0
                        && nodeHash.compareTo(nodeRing.get(i).getNodeHashRange()[1]) < 0) {
                    nodeRing.get(i).setPredecessorHash(nodeHash);
                    node.setPredecessorHash(nodeRing.get(i-1).getNodeHashRange()[1]);
                    nodeRing.add(i, node);
                    break;
                }
            }
        }
        occupiedNode.put(node.getNodeName(), node);
        awaitAddNode(node.getNodeSock().getLocalAddress().getHostAddress() + ":" + node.getServerPort());
    }

    private void awaitAddNode(String listener) {
        TextMessage addMsg = new TextMessage("add " + getMetadata() + " " + listener);
        System.out.println(addMsg.getTextMessage());
        for (ECSNode node : nodeRing) {
            // try {
            //     node.writeOutputStream(addMsg);
            //     node.readInputStream();
            // } catch (IOException ioe) {
            //     System.exit(1);
            // }
        }
    }

    private String getMetadata() {
        StringBuilder metadata = new StringBuilder();
        for (ECSNode node : nodeRing)
            metadata.append(node.getNodeHashRange()[0] + "," + node.getNodeHashRange()[1] + "," + node.getNodeName() + ";");
        return metadata.toString();
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }
}
