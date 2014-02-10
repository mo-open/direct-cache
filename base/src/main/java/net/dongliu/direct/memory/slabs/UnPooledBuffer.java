package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.UnsafeMemory;

/**
 * for large datas. a
 *
 * @author dongliu
 */
public class UnPooledBuffer extends MemoryBuffer {

    private final UnsafeMemory memory;

    private UnPooledBuffer(UnsafeMemory memory) {
        super();
        this.memory = memory;
    }

    public static UnPooledBuffer allocate(int size) {
        UnsafeMemory memory = UnsafeMemory.allocate(size);
        return new UnPooledBuffer(memory);
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
        super.dispose();
        this.memory.dispose();
    }
}