package net.dongliu.directcache.memory;

import net.dongliu.directcache.exception.AllocatorException;
import net.dongliu.directcache.struct.MemoryBuffer;

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


    MemoryBuffer allocate(final int size) throws AllocatorException;

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
