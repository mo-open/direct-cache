package net.dongliu.directcache.memory;

import net.dongliu.directcache.exception.AllocatorException;
import net.dongliu.directcache.struct.MemoryBuffer;

/**
 * Allocator
 * @author dongliu
 */
public interface Allocator {

    /**
     * tryKill the memory buffer.
     * memoryBuffer cannot be use after tryKill.
     * @param memoryBuffer
     */
    void free(final MemoryBuffer memoryBuffer);


    MemoryBuffer allocate(final int size) throws AllocatorException;

    /**
     * destroy allocator, release all resources.
     * after destroy this cannot be used any more
     */
    void destroy();

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
