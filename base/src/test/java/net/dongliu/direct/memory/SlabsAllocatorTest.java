package net.dongliu.direct.memory;

import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.utils.Size;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author: dongliu
 */
public class SlabsAllocatorTest {

    SlabsAllocator allocator;

    @Before
    public void setUp() throws Exception {
        allocator = SlabsAllocator.newInstance(Size.Mb(10));
    }

    @After
    public void tearDown() throws Exception {
        allocator.destroy();
    }

    @Test
    public void testAllocate() throws Exception {
        MemoryBuffer buffer = allocator.allocate(10);
        Assert.assertTrue(buffer.getCapacity() >= 10);
        buffer.dispose();
        Assert.assertEquals(0, allocator.actualUsed());
    }
}
