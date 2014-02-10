package net.dongliu.direct.memory;

import net.dongliu.direct.struct.MemoryBuffer;

import java.nio.BufferOverflowException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Merge a list of Memory to a Big One, as a big memory.
 *
 * @author dongliu
 */
public class MergedMemory {

    final UnsafeMemory[] unsafeMemories;
    private long capacity;
    private final AtomicLong position;

    /**
     * current is 1G
     */
    private static final int MAX_BUFFER_SIZE = 1 << 30;

    private MergedMemory(long totalSize) {
        this.capacity = totalSize;
        this.position = new AtomicLong(0);

        int length = (int) (totalSize / MAX_BUFFER_SIZE);
        int leftSize = (int) (totalSize % MAX_BUFFER_SIZE);
        if (leftSize == 0) {
            leftSize = MAX_BUFFER_SIZE;
        } else {
            length += 1;
        }

        unsafeMemories = new UnsafeMemory[length];
        for (int i = 0; i < length - 1; i++) {
            unsafeMemories[i] = UnsafeMemory.allocate(MAX_BUFFER_SIZE);
        }
        unsafeMemories[length - 1] = UnsafeMemory.allocate(leftSize);
    }

    public static MergedMemory allocate(long totalSize) {
        return new MergedMemory(totalSize);
    }

    public long capacity() {
        return this.capacity;
    }

    /**
     * write data to memory.
     */
    public void write(long start, byte[] data) {
        if (start + data.length > this.capacity) {
            throw new BufferOverflowException();
        }

        int idx = (int) (start / MAX_BUFFER_SIZE);
        int pos = (int) (start % MAX_BUFFER_SIZE);
        if (pos + data.length < MAX_BUFFER_SIZE) {
            unsafeMemories[idx].write(pos, data);
        } else {
            int fsize = MAX_BUFFER_SIZE - pos;
            unsafeMemories[idx].write(pos, data, fsize);
            unsafeMemories[idx].write(pos, data, fsize, data.length - fsize);
        }
    }

    /**
     * read data.
     *
     * @return the data.
     */
    public byte[] read(long start, int size) {
        if (start + size > this.capacity) {
            throw new BufferOverflowException();
        }

        byte[] data = new byte[size];
        int idx = (int) (start / MAX_BUFFER_SIZE);
        int pos = (int) (start % MAX_BUFFER_SIZE);
        if (pos + size < MAX_BUFFER_SIZE) {
            unsafeMemories[idx].read(pos, data);
        } else {
            int fsize = MAX_BUFFER_SIZE - pos;
            unsafeMemories[idx].read(pos, data, fsize);
            unsafeMemories[idx].read(pos, data, fsize, data.length - fsize);
        }
        return data;
    }

    /**
     * get a new memory
     *
     * @param size memory size
     * @return null if do not have enough memory.
     */
    public MemoryBuffer malloc(int size) {
        long newPos = position.addAndGet(size);

        if (newPos > this.capacity) {
            position.addAndGet(-size);
            return null;
        }

        return new MemoryBuffer(this, newPos - size, size);

    }

    /**
     * release all resources.
     */
    public void dispose() {
        for (UnsafeMemory unsafeMemory : unsafeMemories) {
            unsafeMemory.dispose();
        }
    }

}
