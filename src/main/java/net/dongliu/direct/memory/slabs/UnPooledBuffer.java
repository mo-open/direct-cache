package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.UnsafeMemory;

/**
 * direct-memory for large data
 *
 * @author dongliu
 */
public class UnPooledBuffer extends MemoryBuffer {

    private final UnsafeMemory memory;

    /**
     * the allocator which allocate this buf
     */
    private SlabsAllocator allocator;

    private UnPooledBuffer(SlabsAllocator allocator, UnsafeMemory memory) {
        super();
        this.memory = memory;
        this.allocator = allocator;
    }

    public static UnPooledBuffer allocate(SlabsAllocator allocator, int size) {
        UnsafeMemory memory = UnsafeMemory.allocate(size);
        return new UnPooledBuffer(allocator, memory);
    }

    @Override
    public int getCapacity() {
        return this.memory.getSize();
    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public UnsafeMemory getMemory() {
        return this.memory;
    }

    @Override
    public void dispose() {
        this.allocator.used.addAndGet(-memory.getSize());
        this.allocator.actualUsed.addAndGet(-memory.getSize());
        this.memory.dispose();
    }
}