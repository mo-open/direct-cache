package net.dongliu.direct.ehcache;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EHCacheTest {

    static Ehcache ehcache;

    @BeforeClass
    public static void init() {
        CacheManager cacheManager = CacheManager.getInstance();
        ehcache = cacheManager.getEhcache("testCache");
    }

    @Test
    public void testMassPut() {
        for (int i = 0; i < 3000; i++) {
            Element element = new Element(i, new byte[1024]);
            ehcache.put(element);
        }
    }

    @Test
    public void testPutRetreive() {
        ehcache.put(new Element("testKey", "testValue"));
        Assert.assertEquals("testValue", ehcache.get("testKey").getObjectValue());

        ehcache.put(new Element("testKey", 1));
        Assert.assertEquals(1, ehcache.get("testKey").getObjectValue());
    }

    public void testPutNulls() {
        ehcache.put(new Element("testKey", null));
        Assert.assertNull(ehcache.get("testKey").getObjectValue());
        ehcache.put(new Element("testKey", new byte[0]));
        Assert.assertArrayEquals(new byte[0], (byte[]) ehcache.get("testKey").getObjectValue());
    }

    @AfterClass
    public static void destroy() {
    }
}
