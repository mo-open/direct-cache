package net.dongliu.direct.memory;

import net.dongliu.direct.exception.AllocatorException;

/**
 * Allocator
 *
 * @author dongliu
 */
public interface Allocator {

    /**
     * tryKill the memory buffer.
     * memoryBuffer cannot be use after tryKill.
     *
     * @param memoryBuffer
     */
    void free(final MemoryBuffer memoryBuffer);


    MemoryBuffer allocate(final int size) throws AllocatorException;

    /**
     * destroy allocator, release all resources.
     * after destroy this cannot be actualUsed any more
     */
    void destroy();

    /**
     * the capacity
     *
     * @return
     */
    long getCapacity();

    /**
     * the memory size actualUsed.
     *
     * @return
     */
    long actualUsed();
}
