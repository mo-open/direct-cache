package net.dongliu.directmemory.memory;

import net.dongliu.directmemory.struct.MemoryBuffer;

import java.io.Closeable;

/**
 * Allocator
 * @author dongliu
 */
public interface Allocator {

    void free(final MemoryBuffer memoryBuffer);

    MemoryBuffer allocate(final int size);

    void dispose();

    long getCapacity();

    long used();
}
