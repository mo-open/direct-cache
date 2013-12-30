package net.dongliu.directcache.cache;

import net.dongliu.directcache.utils.Size;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author: dongliu
 */
public class BinaryCacheTest {

    private BinaryCache cache;

    @Before
    public void init() {
        cache = new BinaryCache(Size.Mb(100));
    }

    @After
    public void destroy() {
        cache.destroy();
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
