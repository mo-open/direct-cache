package net.dongliu.directmemory.memory;

import net.dongliu.directmemory.struct.MemoryBuffer;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Merge a list of ByteBuffer to a Big One, as a big memory.
 * @author dongliu
 */
public class MergedMemory {

    final ByteBuffer[] byteBuffers;
    private long capacity;
    private final AtomicLong position;

    /** current is 1G */
    private static final int MAX_BUFFER_SIZE = 1 << 30;

    private MergedMemory(long totalSize) {
        this.capacity = totalSize;
        this.position = new AtomicLong(0);

        int count = (int) (totalSize / MAX_BUFFER_SIZE);
        int leftSize = (int) (totalSize % MAX_BUFFER_SIZE);
        if (leftSize == 0) {
            leftSize = MAX_BUFFER_SIZE;
        } else {
            count += 1;
        }

        byteBuffers = new ByteBuffer[count];
        for (int i=0; i < byteBuffers.length - 1; i++) {
            byteBuffers[i] = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
        }
        byteBuffers[byteBuffers.length - 1] = ByteBuffer.allocateDirect(leftSize);
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

        int idx = (int) (start/MAX_BUFFER_SIZE);
        int pos = (int) (start % MAX_BUFFER_SIZE);
        if (pos + data.length < MAX_BUFFER_SIZE) {
            DirectByteBufferUtils.absolutePut(byteBuffers[idx], pos, data, 0, data.length);
        } else {
            int size1 = MAX_BUFFER_SIZE - pos;
            DirectByteBufferUtils.absolutePut(byteBuffers[idx], pos, data, 0, size1);
            DirectByteBufferUtils.absolutePut(byteBuffers[idx + 1], 0, data, size1, data.length - size1);
        }
    }

    /**
     * read data.
     * @return the data.
     */
    public byte[] read(long start, int size) {
        if (start + size > this.capacity) {
            throw new BufferOverflowException();
        }

        byte[] data = new byte[size];
        int idx = (int) (start/MAX_BUFFER_SIZE);
        int pos = (int) (start % MAX_BUFFER_SIZE);
        if (pos + size < MAX_BUFFER_SIZE) {
            DirectByteBufferUtils.absoluteGet(byteBuffers[idx], pos, data, 0, data.length);
        } else {
            int size1 = MAX_BUFFER_SIZE - pos;
            DirectByteBufferUtils.absoluteGet(byteBuffers[idx], pos, data, 0, size1);
            DirectByteBufferUtils.absoluteGet(byteBuffers[idx + 1], 0, data, size1, data.length - size1);
        }
        return data;
    }

    /**
     * get a new memory
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
        for (int i=0; i < byteBuffers.length; i++) {
            byteBuffers[i] = null;
        }
    }

}
