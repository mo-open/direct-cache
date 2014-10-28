package net.dongliu.direct.cache;

import net.dongliu.direct.exception.AllocatorException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.struct.DirectValue;
import net.dongliu.direct.utils.Size;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author: dongliu
 */
public class ConcurrentMapTest {

    private static ConcurrentMap map;
    private static Allocator allocator;

    @BeforeClass
    public static void setup() throws AllocatorException {
        allocator = new SlabsAllocator(Size.Mb(256), 1.25f, 48, Size.Mb(4));
        map = new ConcurrentMap(1000, 0.75f, 16);
    }

    @Test
    public void testSizeAndClear() throws Exception {
        DirectValue holder = new DirectValue(allocator.allocate(1000), "test", String.class);
        map.put("test", holder);
        Assert.assertEquals(1, map.size());
        map.clear();
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.actualUsed());
        map.clear();
        Assert.assertEquals(0, allocator.actualUsed());
    }

    @Test
    public void testGet() throws Exception {
        MemoryBuffer buffer = allocator.allocate(1000);
        byte[] data = "value".getBytes();
        buffer.write(data);
        DirectValue holder = new DirectValue(buffer, "test", String.class);
        map.put("test", holder);
        DirectValue value = map.get("test");
        Assert.assertArrayEquals(data, value.readValue());
        map.clear();
        Assert.assertEquals(0, allocator.actualUsed());
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
        Assert.assertEquals(0, allocator.actualUsed());
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
        Assert.assertEquals(0, allocator.actualUsed());
    }

    @Test
    public void testRemove() throws Exception {
        DirectValue directValue = new DirectValue(allocator.allocate(1000), "test", String.class);
        map.put("test", directValue);
        map.remove("test");
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.actualUsed());
        map.clear();
        Assert.assertEquals(0, allocator.actualUsed());
    }

    @AfterClass
    public static void destroy() {
        map.clear();
        allocator.destroy();
    }
}
