package net.dongliu.direct;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Dong Liu
 */
public class DirectCacheTest {

    @Test
    public void testPut() {
        DirectCache cache = DirectCache.newBuilder().build();
        cache.set("test", "value");
        Value<String> value = cache.get("test", String.class);
        assertTrue(value != null);
        assertEquals("value", value.getValue());
        cache.destroy();
    }

    @Test
    public void testAdd() {
        DirectCache cache = DirectCache.newBuilder().build();
        cache.remove("test");
        boolean f = cache.add("test", "value");
        assertTrue(f);
        f = cache.add("test", "value1");
        assertFalse(f);
        cache.destroy();
    }

    @Test
    public void testNull() {
        DirectCache cache = DirectCache.newBuilder().build();
        cache.set("test", null);
        Value<String> value = cache.get("test", String.class);
        assertNull(value.getValue());
        assertNull(cache.get("test_1234", String.class));
        cache.destroy();

    }

}
