package net.dongliu.direct.memory;

import net.dongliu.direct.exception.AllocatorException;

/**
 * Allocator
 *
 * @author dongliu
 */
public interface Allocator {

    /**
     * allocate memory buf.
     *
     * @param size
     * @return the buf allocated. null if cannot allocate memory due to not enough free memory.
     */
    MemoryBuffer allocate(final int size);

    /**
     * destroy allocator, dispose all resources.
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

    long used();
}
