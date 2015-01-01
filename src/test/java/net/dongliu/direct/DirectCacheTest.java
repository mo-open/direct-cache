package net.dongliu.direct;

import net.dongliu.direct.DirectCache;
import net.dongliu.direct.struct.Value;
import net.dongliu.direct.utils.Size;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Dong Liu
 */
public class DirectCacheTest {

    private static DirectCache<String, String> cache;

    @BeforeClass
    public static void init() {
        cache = DirectCache.<String, String>newBuilder().maxMemorySize(Size.Mb(100)).build();
    }

    @Test
    public void testPut() {
        cache.set("test", "value");
        String value = cache.get("test", String.class).getValue();
        assertEquals("value", value);
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
        Value<String> value = cache.get("test", String.class);
        assertNull(value.getValue());
        assertNull(cache.get("test_1234", String.class));

    }

    @AfterClass
    public static void destroy() {
        cache.destroy();
    }
}
