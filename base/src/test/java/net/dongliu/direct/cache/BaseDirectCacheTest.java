package net.dongliu.direct.cache;

import net.dongliu.direct.utils.Size;
import org.junit.*;

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
        String key = "tst";
        String value = "test2";
        cache.set(key, value.getBytes());
        String value2 = new String(cache.get(key));
        Assert.assertEquals(value, value2);
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
        cache.set("test", null);
        boolean f = cache.replace("test", "value".getBytes());
        Assert.assertTrue(f);
        Assert.assertArrayEquals("value".getBytes(), cache.get("test"));
        cache.remove("test");
        f = cache.replace("test", null);
        Assert.assertFalse(f);
    }

    @AfterClass
    public static void destroy() {
        cache.destroy();
    }
}
