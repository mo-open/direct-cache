package net.dongliu.directmemory.memory;

import net.dongliu.directmemory.struct.MemoryBuffer;

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

    long used();
}
