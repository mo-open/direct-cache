package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.UnsafeMemory;

/**
 * @author dongliu
 */
class Chunk extends MemoryBuffer {
    private final Slab slab;
    private int offset;

    private Chunk(Slab slab, int offset) {
        super();
        this.slab = slab;
        this.offset = offset;
    }

    public static Chunk make(Slab slab, int start) {
        return new Chunk(slab, start);
    }

    @Override
    public int getCapacity() {
        return this.slab.chunkSize;
    }

    @Override
    public int getOffset() {
        return this.offset;
    }

    @Override
    public UnsafeMemory getMemory() {
        return this.slab.getMemory();
    }

    @Override
    public void dispose() {
        this.slab.getSlabClass().freeChunk(this);
    }
}
