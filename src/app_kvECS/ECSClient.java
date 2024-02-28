package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.stream.IntStream;

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
        if (args.length == 2 && args[0].equals("-p")) {
            try {
                int port = Integer.parseInt(args[1]);
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
            System.out.println("ECS> -p <port>");
            System.exit(1);
        }
    }

    public ECSClient(int port) {
        ecsPort = port;
        availableNode = new HashMap<>();
        nodeRing = new ArrayList<>();
        occupiedNode = new HashMap<>();
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
                int count;
                if (token.length > 2)
                    System.out.println("ECS> Error: Too Many Arguments!");
                else if ((count = Integer.parseInt(token[1])) <= 0)
                    System.out.println("ECS> Error: Too Few Nodes!");
                else
                    addNodes(count);
            } catch (NumberFormatException nfe) {
                System.out.println("ECS> Error: Invalid Node Count!");
            }
            break;
        case "remove":
            try {
                int count;
                if (token.length > 2)
                    System.out.println("ECS> Error: Too Many Arguments!");
                else if ((count = Integer.parseInt(token[1])) <= 0)
                    System.out.println("ECS> Error: Too Few Nodes!");
                else
                    removeNodes(count);
            } catch (NumberFormatException nfe) {
                System.out.println("ECS> Error: Invalid Node Count!");
            }
            break;
        case "start":
            if (token.length != 1)
                System.out.println("ECS> Error: Invalid Argument Count!");
            else
                ecsStart();
            break;
        case "stop":
            if (token.length != 1)
                System.out.println("ECS> Error: Invalid Argument Count!");
            else
                ecsStop();
            break;
        default:
            break;
        }
    }

    private void addNodes(int count) {
        if (count > availableNode.size())
            System.out.println("ECS> Error: Too Many Nodes!");
        else
            IntStream.range(0, count).forEach(i -> addNode());
    }

    private void addNode() {
        ECSNode node = new ArrayList<>(availableNode.values()).get(0);
        availableNode.remove(node.getNodeName());
        occupiedNode.put(node.getNodeName(), node);
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
        awaitNode("add", node.getServerAddr() + ":" + node.getServerPort());
        setAvailableNodesMetadata();
    }

    private void awaitNode(String msg, String listener) {
        TextMessage awaitMsg = new TextMessage(msg + " " + getMetadata() + " " + listener);
        for (ECSNode node : occupiedNode.values()) {
            try {
                node.writeOutputStream(awaitMsg);
                node.readInputStream();
            } catch (IOException ioe) {
                System.exit(1);
            }
        }
    }

    private void setAvailableNodesMetadata() {
        TextMessage metadataMsg = new TextMessage("metadata " + getMetadata());
        for (ECSNode node : availableNode.values()) {
            try {
                node.writeOutputStream(metadataMsg);
                node.readInputStream();
            } catch (IOException ignored) {}
        }
    }

    private String getMetadata() {
        StringBuilder metadata = new StringBuilder();
        for (ECSNode node : nodeRing)
            metadata.append(node.getNodeHashRange()[0] + "," + node.getNodeHashRange()[1] + "," + node.getServerAddr() + ":" + node.getServerPort() + ";");
        return metadata.toString();
    }

    private void removeNodes(int count) {
        if (count > occupiedNode.size())
            System.out.println("ECS> Error: Too Many Nodes!");
        else
            IntStream.range(0, count).forEach(i -> removeNode());
    }

    private void removeNode() {
        ECSNode node = new ArrayList<>(occupiedNode.values()).get(0);
        int index = nodeRing.indexOf(node);
        nodeRing.remove(index);
        ECSNode updateNode = nodeRing.get(index == nodeRing.size() ? 0 : index);
        updateNode.setPredecessorHash(node.getNodeHashRange()[0]);
        awaitNode("remove", updateNode.getNodeSock().getLocalAddress().getHostAddress() + ":" + updateNode.getServerPort());
        occupiedNode.remove(node.getNodeName());
        availableNode.put(node.getNodeName(), node);
        setAvailableNodesMetadata();
    }

    private void ecsStart() {
        occupiedNode.forEach((k, v) -> {try {v.writeOutputStream(new TextMessage("start")); v.readInputStream();} catch (IOException ignored) {}});
        availableNode.forEach((k, v) -> {try {v.writeOutputStream(new TextMessage("start")); v.readInputStream();} catch (IOException ignored) {}});
    }

    private void ecsStop() {
        occupiedNode.forEach((k, v) -> {try {v.writeOutputStream(new TextMessage("stop")); v.readInputStream();} catch (IOException ignored) {}});
        availableNode.forEach((k, v) -> {try {v.writeOutputStream(new TextMessage("stop")); v.readInputStream();} catch (IOException ignored) {}});
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
