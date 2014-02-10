package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.UnsafeMemory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongliu
 */
class Slab {
    private final AtomicInteger idx = new AtomicInteger(0);

    public UnsafeMemory getMemory() {
        return memory;
    }

    private UnsafeMemory memory;
    private final int chunkSize;

    private Slab(UnsafeMemory memory, int chunkSize) {
        this.memory = memory;
        this.chunkSize = chunkSize;
    }

    public static Slab make(int size, int chunkSize) {
        UnsafeMemory memory = UnsafeMemory.allocate(size);
        return new Slab(memory, chunkSize);
    }

    /**
     * return next chunk in this slab.
     *
     * @return null if have no chunk left.
     */
    public Chunk nextChunk() {
        int freeChunkIdx = this.idx.getAndIncrement();
        int total = this.memory.getSize() / chunkSize;
        if (freeChunkIdx < total) {
            return Chunk.make(this, freeChunkIdx * chunkSize, chunkSize);
        } else {
            this.idx.getAndDecrement();
            return null;
        }
    }

    public void destroy() {
        this.memory.dispose();
    }
}
