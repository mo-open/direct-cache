package net.dongliu.directcache.cache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author: dongliu
 */
public class BinaryCacheTest {

    private BinaryCache cache;

    @Before
    public void initCache() {
        cache = new BinaryCacheBuilder().setInitialCapacity(1000)
                .setSize(100000)
                .newCacheService();
    }

    @Test
    public void testPut() {
        String key = "tst";
        String value = "test2";
        cache.set(key, value.getBytes());
        String value2 = new String(cache.get(key));
        Assert.assertEquals(value, value2);
    }
}
