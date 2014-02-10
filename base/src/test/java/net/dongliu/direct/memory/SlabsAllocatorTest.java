package net.dongliu.direct.memory;

import net.dongliu.direct.struct.MemoryBuffer;
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
        allocator = SlabsAllocator.getSlabsAllocator(Size.Mb(10));
    }

    @After
    public void tearDown() throws Exception {
        allocator.destroy();
    }

    @Test
    public void testAllocate() throws Exception {
        MemoryBuffer buffer = allocator.allocate(10);
        Assert.assertTrue(buffer.getCapacity() >= 10);

        //allocate a emtpy value buffer
        MemoryBuffer emptyBuffer = allocator.allocate(0);
        Assert.assertEquals(0, emptyBuffer.getCapacity());
    }
}
