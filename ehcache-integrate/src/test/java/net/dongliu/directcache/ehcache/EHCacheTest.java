package net.dongliu.directcache.ehcache;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class EHCacheTest {

    @Test
    public void testSizing() {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache("testCache");
        for (int i = 0; i < 30000; i++) {
            if ((i % 1000) == 0) {
                System.out.println("heatbeat " + i);
                stats(ehcache);
            }
            ehcache.put(new Element(i, new byte[1024]));
        }
        stats(ehcache);
        Assert.assertTrue(true);
    }

    @Test
    public void testOffHeapExceedMemory()
            throws IOException {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache("testCache");
        for (int i = 0; i < 3000; i++) {
            if ((i % 1000) == 0) {
                System.out.println("heatbeat 2 " + i);
                stats(ehcache);
            }
            Element element = new Element(i, new byte[1024]);
            ehcache.put(element);
        }
    }

    @Test
    public void testPutRetreive() {
        CacheManager cacheManager = CacheManager.getInstance();
        Ehcache ehcache = cacheManager.getEhcache("testCache");

        ehcache.put(new Element("testKey", "testValue"));
        stats(ehcache);
        Assert.assertEquals("testValue", ehcache.get("testKey").getObjectValue());


        ehcache.put(new Element("testKey", 1));
        stats(ehcache);
        Assert.assertEquals(1, ehcache.get("testKey").getObjectValue());
    }

    private void stats(Ehcache ehcache) {
        System.out.println("OnHeapSize=" + ehcache.calculateInMemorySize() + ", OnHeapElements="
                + ehcache.getMemoryStoreSize());
        System.out.println("OffHeapSize=" + ehcache.calculateOffHeapSize() + ", OffHeapElements="
                + ehcache.getOffHeapStoreSize());
        System.out.println("DiskStoreSize=" + ehcache.calculateOnDiskSize() + ", DiskStoreElements="
                + ehcache.getDiskStoreSize());
    }

}
