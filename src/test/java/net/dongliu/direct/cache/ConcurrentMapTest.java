package net.dongliu.direct.cache;

import net.dongliu.direct.allocator.Buffer;
import net.dongliu.direct.allocator.NettyAllocator;
import net.dongliu.direct.struct.DirectValue;
import net.dongliu.direct.utils.Size;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Dong Liu
 */
public class ConcurrentMapTest {

    private static ConcurrentMap map;
    private static NettyAllocator allocator;

    @BeforeClass
    public static void setup() {
        allocator = new NettyAllocator(Size.Mb(256));
        map = new ConcurrentMap(1000, 0.75f, 16);
    }

    @Test
    public void testSizeAndClear() throws Exception {
        DirectValue holder = new DirectValue(allocator.allocate(1000), "test", String.class);
        map.put("test", holder);
        Assert.assertEquals(1, map.size());
        map.clear();
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.used());
        map.clear();
        Assert.assertEquals(0, allocator.used());
    }

    @Test
    public void testGet() throws Exception {
        Buffer buffer = allocator.allocate(1000);
        byte[] data = "value".getBytes();
        buffer.write(data);
        DirectValue holder = new DirectValue(buffer, "test", String.class);
        map.put("test", holder);
        DirectValue value = map.get("test");
        Assert.assertArrayEquals(data, value.readValue());
        map.clear();
        Assert.assertEquals(0, allocator.used());
    }

    @Test
    public void testPut() throws Exception {
        DirectValue holder1 = new DirectValue(allocator.allocate(1000), "test", String.class);
        DirectValue holder2 = new DirectValue(allocator.allocate(1001), "test", String.class);
        DirectValue value1 = map.put("test", holder1);
        Assert.assertNull(value1);
        DirectValue value2 = map.put("test", holder2);
        Assert.assertNotNull(value2);
        DirectValue value3 = map.get("test");
        Assert.assertEquals(holder2, value3);
        map.clear();
        Assert.assertEquals(0, allocator.used());
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        DirectValue holder1 = new DirectValue(allocator.allocate(1000), "test", String.class);
        DirectValue holder2 = new DirectValue(allocator.allocate(1001), "test", String.class);
        DirectValue value1 = map.putIfAbsent("test", holder1);
        Assert.assertNull(value1);
        DirectValue value2 = map.putIfAbsent("test", holder2);
        Assert.assertNotNull(value2);
        DirectValue value3 = map.get("test");
        Assert.assertEquals(holder1, value3);
        map.clear();
        Assert.assertEquals(0, allocator.used());
    }

    @Test
    public void testRemove() throws Exception {
        DirectValue directValue = new DirectValue(allocator.allocate(1000), "test", String.class);
        map.put("test", directValue);
        map.remove("test");
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.used());
        map.clear();
        Assert.assertEquals(0, allocator.used());
    }

    @AfterClass
    public static void destroy() {
        map.clear();
    }
}
