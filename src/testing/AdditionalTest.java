package testing;

import java.util.stream.IntStream;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {

    @Test
    public void testLocksPut() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50000);
        runTestPut(kvClient, kvClient_1);
    }

    private void runTestPut(KVStore kvClient, KVStore kvClient_1) {
        try {
            kvClient.connect();
            kvClient_1.connect();
        } catch (Exception ignored) {}
        Thread t0 = new Thread(() -> runTestPut(kvClient, 0, 100));
        Thread t1 = new Thread(() -> runTestPut(kvClient_1, 100, 200));
        t0.start();
        t1.start();
        try {
            t0.join();
            t1.join();
        } catch (Exception ignored) {}
        kvClient.disconnect();
        kvClient_1.disconnect();
    }

    private void runTestPut(KVStore kvClient, int startInclusive, int endExclusive) {
        IntStream.range(startInclusive, endExclusive).forEach(i -> {
            Message response = null;
            Exception ex = null;
            try {
                response = kvClient.put("foo" + i, "bar" + i);
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
        });
    }

    @Test
    public void testLocksUpdate() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50000);
        runTestUpdate(kvClient, kvClient_1);
    }

    private void runTestUpdate(KVStore kvClient, KVStore kvClient_1) {
        try {
            kvClient.connect();
            kvClient_1.connect();
        } catch (Exception ignored) {}
        Thread t0 = new Thread(() -> runTestUpdate(kvClient, 0, 100));
        Thread t1 = new Thread(() -> runTestUpdate(kvClient_1, 100, 200));
        t0.start();
        t1.start();
        try {
            t0.join();
            t1.join();
        } catch (Exception ignored) {}
        kvClient.disconnect();
        kvClient_1.disconnect();
    }

    private void runTestUpdate(KVStore kvClient, int startInclusive, int endExclusive) {
        IntStream.range(startInclusive, endExclusive).forEach(i -> {
            Message response = null;
            Exception ex = null;
            try {
                response = kvClient.put("foo" + i, "update" + i);
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
        });
    }

    @Test
    public void testLocksGet() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50000);
        runTestGet(kvClient, kvClient_1);
    }

    private void runTestGet(KVStore kvClient, KVStore kvClient_1) {
        try {
            kvClient.connect();
            kvClient_1.connect();
        } catch (Exception ignored) {}
        Thread t0 = new Thread(() -> runTestGet(kvClient, 0, 100));
        Thread t1 = new Thread(() -> runTestGet(kvClient_1, 100, 200));
        t0.start();
        t1.start();
        try {
            t0.join();
            t1.join();
        } catch (Exception ignored) {}
        kvClient.disconnect();
        kvClient_1.disconnect();
    }

    private void runTestGet(KVStore kvClient, int startInclusive, int endExclusive) {
        IntStream.range(startInclusive, endExclusive).forEach(i -> {
            Message response = null;
            Exception ex = null;
            try {
                response = kvClient.get("foo" + i);
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
        });
    }

    @Test
    public void testLocksDelete() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50000);
        runTestDelete(kvClient, kvClient_1);
    }

    private void runTestDelete(KVStore kvClient, KVStore kvClient_1) {
        try {
            kvClient.connect();
            kvClient_1.connect();
        } catch (Exception ignored) {}
        Thread t0 = new Thread(() -> runTestDelete(kvClient, 0, 100));
        Thread t1 = new Thread(() -> runTestDelete(kvClient_1, 100, 200));
        t0.start();
        t1.start();
        try {
            t0.join();
            t1.join();
        } catch (Exception ignored) {}
        kvClient.disconnect();
        kvClient_1.disconnect();
    }

    private void runTestDelete(KVStore kvClient, int startInclusive, int endExclusive) {
        IntStream.range(startInclusive, endExclusive).forEach(i -> {
            Message response = null;
            Exception ex = null;
            try {
                response = kvClient.put("foo" + i, "null");
            } catch (Exception e) {
                ex = e;
            }
            assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
        });
    }

    @Test
    public void testRetryOperationsPut() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50001);
        runTestPut(kvClient, kvClient_1);
    }

    @Test
    public void testRetryOperationsUpdate() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50001);
        runTestUpdate(kvClient, kvClient_1);
    }

    @Test
    public void testRetryOperationsGet() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50001);
        runTestGet(kvClient, kvClient_1);
    }

    @Test
    public void testRetryOperationsDelete() {
        KVStore kvClient = new KVStore("localhost", 50000);
        KVStore kvClient_1 = new KVStore("localhost", 50001);
        runTestDelete(kvClient, kvClient_1);
    }
}