package net.dongliu.directmemory.memory.allocator;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

/**
 * Merge a list of ByteBuffer to a Big One, as a big memory.
 * @author dongliu
 */
public class MergedMemory {

    final ByteBuffer[] byteBuffers;
    private long capacity;
    private long position;

    /** current is 1G */
    private static final int MAX_BUFFER_SIZE = 1 << 30;

    private MergedMemory(long totalSize) {
        this.capacity = totalSize;

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
            byteBuffers[idx].position(pos);
            byteBuffers[idx].put(data);
        } else {
            int size1 = MAX_BUFFER_SIZE - pos;
            byteBuffers[idx].put(data, 0, size1);
            byteBuffers[idx + 1].put(data, size1, data.length - size1);
        }
    }

    /**
     * read data.
     * @return
     */
    public byte[] read(long start, int size) {
        if (start + size > this.capacity) {
            throw new BufferOverflowException();
        }

        byte[] data = new byte[size];
        int idx = (int) (start/MAX_BUFFER_SIZE);
        int pos = (int) (start % MAX_BUFFER_SIZE);
        if (pos + size < MAX_BUFFER_SIZE) {
            byteBuffers[idx].position(pos);
            byteBuffers[idx].get(data);
        } else {
            int size1 = MAX_BUFFER_SIZE - pos;
            byteBuffers[idx].get(data, 0, size1);
            byteBuffers[idx + 1].get(data, size1, data.length - size1);
        }
        return data;
    }

    public void close() {
        for (int i=0; i < byteBuffers.length; i++) {
            byteBuffers[i] = null;
        }
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

}
