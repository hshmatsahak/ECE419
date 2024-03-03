package testing;

import java.util.stream.IntStream;
import java.util.ArrayList;

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

    @Test
    public void testPerf_2() {
        runTestPerf(2);
    }

    @Test
    public void testPerf_4() {
        runTestPerf(4);
    }

    @Test
    public void testPerf_8() {
        runTestPerf(8);
    }

    private void runTestPerf(int serverCount) {
        KVStore[] kvClient = new KVStore[8];
        IntStream.range(0, kvClient.length).forEach(i -> {
            kvClient[i] = new KVStore("localhost", 50000 + i % serverCount);
            try {kvClient[i].connect();} catch (Exception ignored) {}
        });
        runTestPerf(kvClient, 2);
        runTestPerf(kvClient, 4);
        runTestPerf(kvClient, 8);
    }

    private void runTestPerf(KVStore[] kvClient, int clientCount) {
        Thread[] put = new Thread[clientCount];
        IntStream.range(0, clientCount).forEach(i ->
            put[i] = new Thread(() -> runTestPut(kvClient[i], i*1000, (i+1)*1000))
        );
        long time = System.currentTimeMillis();
        runTestPerf(put);
        System.out.println((System.currentTimeMillis() - time));
        Thread[] get = new Thread[clientCount];
        IntStream.range(0, clientCount).forEach(i ->
            get[i] = new Thread(() -> runTestGet(kvClient[i], i*1000, (i+1)*1000))
        );
        time = System.currentTimeMillis();
        runTestPerf(get);
        System.out.println((System.currentTimeMillis() - time));
        Thread[] delete = new Thread[clientCount];
        IntStream.range(0, clientCount).forEach(i ->
            delete[i] = new Thread(() -> runTestDelete(kvClient[i], i*1000, (i+1)*1000))
        );
        runTestPerf(delete);
    }

    private void runTestPerf(Thread[] t) {
        IntStream.range(0, t.length).forEach(i -> t[i].start());
        IntStream.range(0, t.length).forEach(i -> {try {t[i].join();} catch (Exception ignored) {}});
    }
}