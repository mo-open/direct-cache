package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.UnsafeMemory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongliu
 */
class Slab {
    private final AtomicInteger idx = new AtomicInteger(0);
    private final SlabClass slabClass;
    private UnsafeMemory memory;
    protected final int chunkSize;

    private Slab(SlabClass slabClass, UnsafeMemory memory, int chunkSize) {
        this.slabClass = slabClass;
        this.memory = memory;
        this.chunkSize = chunkSize;
    }

    public static Slab newInstance(SlabClass slabClass, int size, int chunkSize) {
        UnsafeMemory memory = UnsafeMemory.allocate(size);
        return new Slab(slabClass, memory, chunkSize);
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
            return Chunk.make(this, freeChunkIdx * chunkSize);
        } else {
            this.idx.getAndDecrement();
            return null;
        }
    }

    public void destroy() {
        this.memory.dispose();
    }

    public SlabClass getSlabClass() {
        return slabClass;
    }

    public UnsafeMemory getMemory() {
        return memory;
    }
}
