package net.dongliu.direct.cache;

import net.dongliu.direct.utils.Size;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author: dongliu
 */
public class BaseDirectCacheTest {

    private static BaseDirectCache cache;

    @BeforeClass
    public static void init() {
        cache = new BaseDirectCache(Size.Mb(100));
    }

    @Test
    public void testPut() {
        cache.set("test", "value".getBytes());
        String value = new String(cache.get("test"));
        Assert.assertEquals("value", value);
    }

    @Test
    public void testPutNullAndEmpty() {
        cache.set("test", null);
        Assert.assertTrue(cache.exists("test"));
        Assert.assertNull(cache.get("test"));
        cache.set("test", new byte[0]);
        Assert.assertTrue(cache.exists("test"));
        Assert.assertArrayEquals(new byte[0], cache.get("test"));
    }

    @Test
    public void testAdd() {
        cache.remove("test");
        boolean f = cache.add("test", null);
        Assert.assertTrue(f);
        f = cache.add("test", "value".getBytes());
        Assert.assertFalse(f);
    }

    @Test
    public void testReplace() {
        cache.set("test", "".getBytes());
        byte[] value = cache.replace("test", "value".getBytes());
        Assert.assertArrayEquals("".getBytes(), value);
        Assert.assertArrayEquals("value".getBytes(), cache.get("test"));
        cache.remove("test");
        value = cache.replace("test", null);
        Assert.assertNull(value);
    }

    @AfterClass
    public static void destroy() {
        cache.destroy();
    }
}
