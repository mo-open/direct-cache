package net.dongliu.direct.ehcache;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.junit.Assert;
import org.junit.Test;

public class EHCacheTest {

    @Test
    public void testMassPut() {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache("testCache");
        for (int i = 0; i < 3000; i++) {
            Element element = new Element(i, new byte[1024]);
            ehcache.put(element);
        }
    }

    @Test
    public void testPutRetreive() {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache("testCache");

        ehcache.put(new Element("testKey", "testValue"));
        Assert.assertEquals("testValue", ehcache.get("testKey").getObjectValue());


        ehcache.put(new Element("testKey", 1));
        Assert.assertEquals(1, ehcache.get("testKey").getObjectValue());
    }

}
