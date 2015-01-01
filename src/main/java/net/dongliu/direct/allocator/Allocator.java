package net.dongliu.direct.allocator;


import net.dongliu.direct.utils.Size;

import java.util.concurrent.atomic.AtomicLong;

/**
 * buffer allocator use netty buffer
 *
 * @author Dong Liu
 */
public class Allocator {

    private final ByteBufAllocator allocator;
    private final long capacity;
    private final AtomicLong used = new AtomicLong(0);

    public final ByteBuf nullBuf;

    public Allocator(long capacity, int arenaNum) {
        this.capacity = capacity;
        this.allocator = new ByteBufAllocator(arenaNum, Size.Kb(8), 11);
        this.nullBuf = allocator.directBuffer(8, 8);
    }

    /**
     * allocate new buffer
     *
     * @return null if not enough memory
     */
//    public ByteBuf allocate(int size) {
//        if (used.get() > capacity) {
//            return null;
//        }
//        ByteBuf buf = allocator.directBuffer(size, size);
//        used.addAndGet(buf.capacity());
//        return buf;
//    }

    /**
     * allocate new buffer
     *
     * @return null if not enough memory
     */
    public ByteBuf newBuffer(byte[] bytes) {
        if (used.get() > capacity) {
            return null;
        }

        if (bytes == null) {
            return nullBuf;
        } else {
            ByteBuf buf = allocator.directBuffer(bytes.length, bytes.length);
            used.addAndGet(buf.capacity());
            buf.writeBytes(bytes);
            return buf;
        }
    }

    public long getCapacity() {
        return this.capacity;
    }

    public long used() {
        return this.used.get();
    }

    public void release(ByteBuf buf) {
        if (buf == nullBuf) {
            return;
        }
        if (buf.release()) {
            used.addAndGet(-buf.capacity());
        }
    }
}
