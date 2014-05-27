package net.dongliu.direct.cache;

import net.dongliu.direct.serialization.DefaultSerializer;
import net.dongliu.direct.utils.Size;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author: dongliu
 */
public class DirectCacheTest {

    private static DirectCache cache;

    private static DefaultSerializer serializer = new DefaultSerializer();

    @BeforeClass
    public static void init() {
        cache = DirectCache.newBuilder().maxMemorySize(Size.Mb(100)).build();
    }

    @Test
    public void testPut() {
        cache.set("test", "value", serializer);
        String value = (String) cache.get("test", serializer);
        Assert.assertEquals("value", value);
    }

    @Test
    public void testAdd() {
        cache.remove("test");
        boolean f = cache.add("test", "value", serializer);
        Assert.assertTrue(f);
        f = cache.add("test", "value1", serializer);
        Assert.assertFalse(f);
    }

    @Test
    public void testReplace() {
        cache.set("test", "", serializer);
        String value = (String) cache.replace("test", "value", serializer);
        Assert.assertEquals("", value);
        Assert.assertEquals("value", cache.get("test", serializer));
        cache.remove("test");
        value = (String) cache.replace("test", "value1", serializer);
        Assert.assertNull(value);
    }

    @AfterClass
    public static void destroy() {
        cache.destroy();
    }
}
