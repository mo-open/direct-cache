package net.dongliu.direct.allocator;


import net.dongliu.direct.buffer.ByteBuf;
import net.dongliu.direct.buffer.PooledByteBufAllocator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dong Liu
 */
public class NettyAllocator {

    private PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    private final long capacity;

    private final AtomicLong used = new AtomicLong(0);

    public NettyAllocator(long capacity) {
        this.capacity = capacity;
    }

    /**
     * allocate new buffer
     *
     * @param size
     * @return null if not enough memory
     */
    public Buffer allocate(int size) {
        if (used.addAndGet(size) > capacity) {
            used.decrementAndGet();
            return null;
        }
        ByteBuf buf = allocator.directBuffer(size);
        return new Buffer(this, buf);
    }

    public long getCapacity() {
        return this.capacity;
    }

    public long used() {
        return this.used.get();
    }

    protected void release(ByteBuf buf) {
        buf.release();
        used.addAndGet(-buf.capacity());
    }
}
