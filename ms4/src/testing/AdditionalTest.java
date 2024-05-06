package testing;

import java.util.stream.IntStream;
import java.util.ArrayList;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {

    private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception ignored) {}
    }

    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPut() {
        Message response = null;
        Exception ex = null;
        try {
            response = kvClient.put("u1", "foo", "bar");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
    }

    @Test
    public void testGetErr() {
        Message response = null;
        Exception ex = null;
        try {
            response = kvClient.get("u2", "foo");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }

    @Test
    public void testGet() {
        Message response = null;
        Exception ex = null;
        try {
            response = kvClient.get("u1", "foo");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
    }

    @Test
    public void testUpdateErr() {
        Message response = null;
        Exception ex = null;
        try {
            response = kvClient.put("u2", "foo", "bar update");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
    }

    @Test
    public void testUpdate() {
        Message response = null;
        Exception ex = null;
        try {
            response = kvClient.put("u1", "foo", "bar update");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
    }

    @Test
    public void testDeleteErr() {
        Message response = null;
        Exception ex = null;
        try {
            response = kvClient.put("u2", "foo", "null");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
    }

    @Test
    public void testDelete() {
        Message response = null;
        Exception ex = null;
        try {
            response = kvClient.put("u1", "foo", "null");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
    }

    @Test
    public void testPutTwo() {}

    @Test
    public void testGetTwo() {}

    @Test
    public void testDeleteTwo() {}
}