package net.dongliu.directmemory.memory;

import net.dongliu.directmemory.struct.MemoryBuffer;

import java.io.Closeable;

/**
 * Allocator
 * @author dongliu
 */
public interface Allocator {

    /**
     * returnTo the memory buffer.
     * memoryBuffer cannot be use after returnTo.
     * @param memoryBuffer
     */
    void free(final MemoryBuffer memoryBuffer);


    MemoryBuffer allocate(final int size);

    /**
     * destroy allocator, release all resources.
     * after dispose this cannot be used any more
     */
    void dispose();

    /**
     * the capacity
     * @return
     */
    long getCapacity();

    /**
     * the memory size used.
     * @return
     */
    long used();
}
