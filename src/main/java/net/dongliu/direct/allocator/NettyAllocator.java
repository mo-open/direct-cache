package net.dongliu.direct.allocator;


import net.dongliu.direct.buffer.ByteBuf;
import net.dongliu.direct.buffer.PooledByteBufAllocator;
import net.dongliu.direct.utils.Size;

import java.util.concurrent.atomic.AtomicLong;

/**
 * buffer allocator use netty buffer
 *
 * @author Dong Liu
 */
public class NettyAllocator {

    private final PooledByteBufAllocator allocator;

    private final long capacity;

    private final AtomicLong used = new AtomicLong(0);

    public NettyAllocator(long capacity, int arenaNum) {
        this.capacity = capacity;
        this.allocator = new PooledByteBufAllocator(arenaNum, Size.Kb(8), 11);
    }

    /**
     * allocate new buffer
     *
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
