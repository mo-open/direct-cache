package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.UnsafeMemory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongliu
 */
class Slab extends MemoryBuffer {
    private final AtomicInteger idx = new AtomicInteger(0);

    private final int chunkSize;

    private Slab(UnsafeMemory memory, int offset, int capacity, int chunkSize) {
        super(memory, offset, capacity);
        this.chunkSize = chunkSize;
    }

    public static Slab make(MemoryBuffer buffer, int chunkSize) {
        return new Slab(buffer.getMemory(), buffer.getOffset(), buffer.getCapacity(), chunkSize);
    }

    /**
     * return next chunk in this slab.
     *
     * @return null if have no chunk left.
     */
    public Chunk nextChunk() {
        int freeChunkIdx = this.idx.getAndIncrement();
        int total = this.getCapacity() / chunkSize;
        if (freeChunkIdx < total) {
            return Chunk.make(this, freeChunkIdx * chunkSize, chunkSize);
        } else {
            this.idx.getAndDecrement();
            return null;
        }
    }
}
