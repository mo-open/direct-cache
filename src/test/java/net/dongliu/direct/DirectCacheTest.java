package net.dongliu.direct;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Dong Liu
 */
public class DirectCacheTest {

    private static DirectCache cache;

    @BeforeClass
    public static void init() {
        cache = DirectCache.newBuilder().build();
    }

    @Test
    public void testPut() {
        cache.set("test", "value");
        Value<String> value = cache.get("test");
        assertTrue(value.present());
        String str = value.getValue();
        assertEquals("value", str);
    }

    @Test
    public void testAdd() {
        cache.remove("test");
        boolean f = cache.add("test", "value");
        assertTrue(f);
        f = cache.add("test", "value1");
        assertFalse(f);
    }

    @Test
    public void testNull() {
        cache.set("test", null);
        Value<String> value = cache.get("test");
        assertNull(value.getValue());
        assertNull(cache.get("test_1234"));

    }

    @AfterClass
    public static void destroy() {
        cache.destroy();
    }
}
