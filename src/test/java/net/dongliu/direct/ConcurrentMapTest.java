package net.dongliu.direct;

import net.dongliu.direct.allocator.ByteBuf;
import net.dongliu.direct.allocator.Allocator;
import net.dongliu.direct.value.DirectValue;
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
    private static Allocator allocator;

    @BeforeClass
    public static void setup() {
        allocator = new Allocator(Size.Mb(256));
        map = new ConcurrentMap(1000, 0.75f, 1);
    }

    @Test
    public void testSizeAndClear() throws Exception {
        DirectValue holder = new DirectValue(newBuffer("value123".getBytes()), "test");
        map.put("test", holder);
        Assert.assertEquals(1, map.size());
        map.clear();
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.getUsed().get());
        map.clear();
        Assert.assertEquals(0, allocator.getUsed().get());
    }

    @Test
    public void testGet() throws Exception {
        byte[] data = "value".getBytes();
        ByteBuf buffer = newBuffer(data);
        DirectValue holder = new DirectValue(buffer, "test");
        map.put("test", holder);
        DirectValue value = map.get("test");
        Assert.assertArrayEquals(data, value.readValue());
        map.clear();
        Assert.assertEquals(0, allocator.getUsed().get());
    }

    @Test
    public void testPut() throws Exception {
        DirectValue holder1 = new DirectValue(newBuffer("value1".getBytes()), "test");
        DirectValue holder2 = new DirectValue(newBuffer("value23".getBytes()), "test");
        DirectValue value1 = map.put("test", holder1);
        Assert.assertNull(value1);
        DirectValue value2 = map.put("test", holder2);
        Assert.assertNotNull(value2);
        DirectValue value3 = map.get("test");
        Assert.assertEquals(holder2, value3);
        map.clear();
        Assert.assertEquals(0, allocator.getUsed().get());
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        DirectValue holder1 = new DirectValue(newBuffer("value1".getBytes()), "test");
        DirectValue holder2 = new DirectValue(newBuffer("value23".getBytes()), "test");
        DirectValue value1 = map.putIfAbsent("test", holder1);
        Assert.assertNull(value1);
        DirectValue value2 = map.putIfAbsent("test", holder2);
        Assert.assertNotNull(value2);
        DirectValue value3 = map.get("test");
        Assert.assertEquals(holder1, value3);
        map.clear();
        Assert.assertEquals(0, allocator.getUsed().get());
    }

    @Test
    public void testRemove() throws Exception {
        DirectValue directValue = new DirectValue(newBuffer("value1".getBytes()), "test");
        map.put("test", directValue);
        map.remove("test");
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, allocator.getUsed().get());
        map.clear();
        Assert.assertEquals(0, allocator.getUsed().get());
    }

    private ByteBuf newBuffer(byte[] bytes) {
        ByteBuf buf = allocator.directBuffer(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    @AfterClass
    public static void destroy() {
        map.clear();
    }
}
