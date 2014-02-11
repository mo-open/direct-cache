package net.dongliu.direct.cache;

import net.dongliu.direct.utils.Size;
import org.junit.*;

/**
 * @author: dongliu
 */
public class BinaryCacheTest {

    private static BinaryCache cache;

    @BeforeClass
    public static void init() {
        cache = new BinaryCache(Size.Mb(100));
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

    @AfterClass
    public static void destroy() {
        cache.destroy();
    }
}
