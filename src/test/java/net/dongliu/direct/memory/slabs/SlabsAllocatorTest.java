package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.utils.Size;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author dongliu
 */
public class SlabsAllocatorTest {
    SlabsAllocator allocator;

    @Before
    public void init() {
        allocator = new SlabsAllocator(Size.Mb(256), 1.25f, 48, Size.Mb(4));
    }

    @Test
    public void testAllocate() throws Exception {
        MemoryBuffer buf = allocator.allocate(1000);
        Assert.assertTrue(1000 <= buf.capacity());
        Assert.assertEquals(buf.capacity(), allocator.actualUsed());
        buf.dispose();
        Assert.assertEquals(0, allocator.actualUsed());
    }

    @Test
    public void testMassAllocate() throws Exception {
        MemoryBuffer buf = allocator.allocate(Size.Mb(50));
        Assert.assertEquals(Size.Mb(50), buf.capacity());
        Assert.assertEquals(buf.capacity(), allocator.actualUsed());

        buf.dispose();
        Assert.assertEquals(0, allocator.actualUsed());
        Assert.assertEquals(0, allocator.used());
    }

    @After
    public void destroy() {
        allocator.destroy();
    }
}
