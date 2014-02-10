package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.utils.Size;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author dongliu
 */
public class SlabsAllocatorTest {
    @Test
    public void testAllocate() throws Exception {
        SlabsAllocator slabsAllocator = SlabsAllocator.newInstance(Size.Mb(256));
        MemoryBuffer buf = slabsAllocator.allocate(1000);
        Assert.assertTrue(1000 < buf.getCapacity());
        Assert.assertEquals(buf.getCapacity(), slabsAllocator.actualUsed());

        slabsAllocator.free(buf);
        Assert.assertEquals(0, slabsAllocator.actualUsed());

    }
}
