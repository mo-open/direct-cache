package net.dongliu.direct.memory.je;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author dongliu
 */
public class PooledByteBufAllocatorTest {
    @Test
    public void testAllocate() throws Exception {

        PooledByteBuf buf = PooledByteBufAllocator.DEFAULT.allocate(1000);
        Assert.assertNotNull(buf);
    }
}
