package net.dongliu.directmemory.memory.allocator;

import net.dongliu.directmemory.memory.struct.MemoryBuffer;

import java.io.Closeable;

/**
 * Allocator
 * @author dongliu
 */
public interface Allocator extends Closeable {

    void free(final MemoryBuffer memoryBuffer);

    MemoryBuffer allocate(final int size);

    @Override
    void close();

    long getCapacity();

}
