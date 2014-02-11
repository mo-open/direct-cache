package net.dongliu.direct.cache;

import net.dongliu.direct.exception.AllocatorException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.struct.BaseValueHolder;
import net.dongliu.direct.struct.ValueHolder;
import net.dongliu.direct.utils.Size;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author: dongliu
 */
public class CacheConcurrentHashMapTest {

    private CacheConcurrentHashMap map;
    private Allocator allocator;

    @Before
    public void setup() throws AllocatorException {
        allocator = SlabsAllocator.newInstance(Size.Mb(10));
        map = new CacheConcurrentHashMap(1000, 0.75f, 16, null);
    }

    @After
    public void destroy() {
        map.clear();
        allocator.destroy();
    }

    @Test
    public void testSizeAndClear() throws Exception {
        Assert.assertEquals(0, map.size());
        BaseValueHolder valueWrapper1 = new BaseValueHolder(allocator.allocate(1000));
        map.put("test", valueWrapper1);
        Assert.assertEquals(1, map.size());
        map.clear();
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.actualUsed());
    }

    @Test
    public void testGet() throws Exception {
        MemoryBuffer buffer = allocator.allocate(1000);
        byte[] data = "value".getBytes();
        buffer.write(data);
        BaseValueHolder valueWrapper1 = new BaseValueHolder(buffer);
        map.put("test", valueWrapper1);
        ValueHolder value = map.get("test");
        Assert.assertArrayEquals(data, value.readValue());
        map.clear();
    }

    @Test
    public void testPut() throws Exception {
        BaseValueHolder valueWrapper1 = new BaseValueHolder(allocator.allocate(1000));
        BaseValueHolder valueWrapper2 = new BaseValueHolder(allocator.allocate(1001));
        ValueHolder value1 = map.put("test", valueWrapper1);
        Assert.assertNull(value1);
        ValueHolder value2 = map.put("test", valueWrapper2);
        Assert.assertNotNull(value2);
        ValueHolder value3 = map.get("test");
        Assert.assertEquals(valueWrapper2, value3);
        map.clear();
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        BaseValueHolder valueWrapper1 = new BaseValueHolder(allocator.allocate(1000));
        BaseValueHolder valueWrapper2 = new BaseValueHolder(allocator.allocate(1001));
        ValueHolder value1 = map.putIfAbsent("test", valueWrapper1);
        Assert.assertNull(value1);
        ValueHolder value2 = map.putIfAbsent("test", valueWrapper2);
        Assert.assertNotNull(value2);
        ValueHolder value3 = map.get("test");
        Assert.assertEquals(valueWrapper1, value3);
    }

    @Test
    public void testRemove() throws Exception {
        BaseValueHolder valueWrapper1 = new BaseValueHolder(allocator.allocate(1000));
        map.put("test", valueWrapper1);
        map.remove("test");
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.actualUsed());
    }

}
