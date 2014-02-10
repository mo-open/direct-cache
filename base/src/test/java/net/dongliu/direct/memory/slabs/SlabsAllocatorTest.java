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
        allocator = SlabsAllocator.newInstance(Size.Mb(256));
    }

    @Test
    public void testAllocate() throws Exception {
        MemoryBuffer buf = allocator.allocate(1000);
        Assert.assertTrue(1000 < buf.getCapacity());
        Assert.assertEquals(buf.getCapacity(), allocator.actualUsed());

        allocator.free(buf);
        Assert.assertEquals(0, allocator.actualUsed());
    }

    @Test
    public void testMassAllocate() throws Exception {
        MemoryBuffer buf = allocator.allocate(Size.Mb(20));
        Assert.assertEquals(Size.Mb(20), buf.getCapacity());
        Assert.assertEquals(buf.getCapacity(), allocator.actualUsed());

        allocator.free(buf);
        Assert.assertEquals(0, allocator.actualUsed());
        Assert.assertEquals(0, allocator.used());
    }

    @After
    public void destroy() {
        allocator.destroy();
    }
}
