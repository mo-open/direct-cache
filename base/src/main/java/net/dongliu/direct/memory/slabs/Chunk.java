package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.UnsafeMemory;

/**
 * @author dongliu
 */
class Chunk extends MemoryBuffer {
    public static Chunk make(Slab slab, int start, int capacity) {
        return new Chunk(slab, start, capacity);
    }

    private Chunk(Slab slab, int start, int capacity) {
        this(slab.getMemory(), slab.getOffset() + start, capacity);
    }

    private Chunk(UnsafeMemory memory, int start, int size) {
        super(memory, start, size);
    }
}
