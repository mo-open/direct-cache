package net.dongliu.directcache.memory;

import org.junit.Assert;
import net.dongliu.directcache.struct.MemoryBuffer;
import net.dongliu.directcache.utils.Size;
import org.junit.After;
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
