package net.dongliu.direct.cache;

import net.dongliu.direct.exception.AllocatorException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.struct.BaseValueHolder;
import net.dongliu.direct.struct.ValueHolder;
import net.dongliu.direct.utils.Size;
import org.junit.*;

/**
 * @author: dongliu
 */
public class CacheMapTest {

    private static CacheMap map;
    private static Allocator allocator;

    @BeforeClass
    public static void setup() throws AllocatorException {
        allocator = SlabsAllocator.newInstance(Size.Mb(10));
        map = new CacheMap(1000, 0.75f, 16, null);
    }

    @Test
    public void testSizeAndClear() throws Exception {
        map.clear();
        Assert.assertEquals(0, map.size());
        BaseValueHolder holder = new BaseValueHolder(allocator.allocate(1000));
        map.put("test", holder);
        Assert.assertEquals(1, map.size());
        map.clear();
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.actualUsed());
        Assert.assertFalse(holder.isLive());
    }

    @Test
    public void testGet() throws Exception {
        map.clear();
        MemoryBuffer buffer = allocator.allocate(1000);
        byte[] data = "value".getBytes();
        buffer.write(data);
        BaseValueHolder holder = new BaseValueHolder(buffer);
        map.put("test", holder);
        ValueHolder value = map.get("test");
        Assert.assertArrayEquals(data, value.readValue());
    }

    @Test
    public void testPut() throws Exception {
        map.clear();
        BaseValueHolder holder1 = new BaseValueHolder(allocator.allocate(1000));
        BaseValueHolder holder2 = new BaseValueHolder(allocator.allocate(1001));
        ValueHolder value1 = map.put("test", holder1);
        Assert.assertNull(value1);
        ValueHolder value2 = map.put("test", holder2);
        Assert.assertNotNull(value2);
        ValueHolder value3 = map.get("test");
        Assert.assertEquals(holder2, value3);
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        map.clear();
        BaseValueHolder holder1 = new BaseValueHolder(allocator.allocate(1000));
        BaseValueHolder holder2 = new BaseValueHolder(allocator.allocate(1001));
        ValueHolder value1 = map.putIfAbsent("test", holder1);
        Assert.assertNull(value1);
        ValueHolder value2 = map.putIfAbsent("test", holder2);
        Assert.assertNotNull(value2);
        ValueHolder value3 = map.get("test");
        Assert.assertEquals(holder1, value3);
    }

    @Test
    public void testRemove() throws Exception {
        map.clear();
        Assert.assertEquals(0, allocator.actualUsed());
        BaseValueHolder valueHolder = new BaseValueHolder(allocator.allocate(1000));
        map.put("test", valueHolder);
        map.remove("test");
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.actualUsed());
    }

    @AfterClass
    public static void destroy() {
        map.clear();
        allocator.destroy();
    }
}
