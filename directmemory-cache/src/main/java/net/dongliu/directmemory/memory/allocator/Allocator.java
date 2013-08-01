package net.dongliu.directmemory.memory.allocator;

import net.dongliu.directmemory.memory.buffer.MemoryBuffer;

import java.io.Closeable;

/**
 * Interface defining interaction with {@link MemoryBuffer}
 *
 * @since 0.6
 */
public interface Allocator extends Closeable {

    /**
     * Returns the given {@link MemoryBuffer} making it available for a future usage.
     * Returning twice a {@link MemoryBuffer} won't throw an exception.
     *
     * @param memoryBuffer : the {@link MemoryBuffer} to return
     */
    void free(final MemoryBuffer memoryBuffer);

    /**
     * Allocates and returns a {@link MemoryBuffer} with {@link MemoryBuffer#capacity()} set to the given size.
     * When the allocation fails, it returns either null or throws an {@link BufferOverflowException},
     * depending on the implementation.
     *
     * @param size : the size in byte to allocate
     * @return a {@link MemoryBuffer} of the given size, or either return null
     *          or throw an {@link BufferOverflowException} when the allocation fails.
     */
    MemoryBuffer allocate(final int size);

    /**
     * Clear all allocated {@link MemoryBuffer}, resulting in a empty and ready to deserve {@link Allocator}
     */
    void clear();

    /**
     * @return the internal total size that can be allocated
     */
    int getCapacity();


}
