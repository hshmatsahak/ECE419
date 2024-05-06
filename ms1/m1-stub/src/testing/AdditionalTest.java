package testing;

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
	public void testLongPut() {
		Message response = null;
		Exception ex = null;
		try {
			response = kvClient.put("bar", "long foo");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testLongUpdate() {
		Message response = null;
		Exception ex = null;
		try {
			response = kvClient.put("bar", "long foo updated");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				   && response.getValue().equals("long foo updated"));
	}

	@Test
	public void testLongGet() {
		Message response = null;
		Exception ex = null;
		try {
			response = kvClient.get("bar");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS
				   && response.getValue().equals("long foo updated"));
	}

	@Test
	public void testLongDelete() {
		Message response = null;
		Exception ex = null;
		try {
			response = kvClient.put("bar", "null");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testDeleteUnsetValue() {
		Message response = null;
		Exception ex = null;
		try {
			response = kvClient.put("an_unset_value", "null");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}

	@Test
	public void testGetValueIsKey1() {
		Message response = null;
		Exception ex = null;
		try {
			kvClient.put("bar", "bar");
			response = kvClient.get("bar");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
	}

	@Test
	public void testGetValueIsKey2() {
		Message response = null;
		Exception ex = null;
		try {
			kvClient.put("bar", "null");
			response = kvClient.get("bar");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	public void test80put20get() {
		Message response = null;
		Exception ex = null;
		try {
			long start = System.currentTimeMillis();
			for (int i=0; i<800; ++i) {
				response = kvClient.put("bar" + String.valueOf(i), "foo");
				assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			}
			for (int i=0; i<200; ++i) {
				response = kvClient.get("bar" + String.valueOf(i));
				assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
			}
			long end = System.currentTimeMillis();
			System.out.println("test80put20get " + String.valueOf(end - start));
			for (int i=0; i<800; ++i)
				kvClient.put("bar" + String.valueOf(i), "null");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	@Test
	public void test50put50get() {
		Message response = null;
		Exception ex = null;
		try {
			long start = System.currentTimeMillis();
			for (int i=0; i<500; ++i) {
				response = kvClient.put("bar" + String.valueOf(i), "foo");
				assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			}
			for (int i=0; i<500; ++i) {
				response = kvClient.get("bar" + String.valueOf(i));
				assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
			}
			long end = System.currentTimeMillis();
			System.out.println("test50put50get " + String.valueOf(end - start));
			for (int i=0; i<500; ++i)
				kvClient.put("bar" + String.valueOf(i), "null");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}

	@Test
	public void test20put80get() {
		Message response = null;
		Exception ex = null;
		try {
			long start = System.currentTimeMillis();
			for (int i=0; i<200; ++i) {
				response = kvClient.put("bar" + String.valueOf(i), "foo");
				assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			}
			for (int i=0; i<800; ++i) {
				response = kvClient.get("bar" + String.valueOf(i % 200));
				assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
			}
			long end = System.currentTimeMillis();
			System.out.println("test20put80get " + String.valueOf(end - start));
			for (int i=0; i<200; ++i)
				kvClient.put("bar" + String.valueOf(i), "null");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);
	}
	
	@Test
	public void testStub() {
		assertTrue(true);
	}
}
