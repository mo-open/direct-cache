package net.dongliu.direct.memory;

import java.nio.BufferOverflowException;

/**
 * a memory area.
 *
 * @author dongliu
 */
public class MemoryBuffer {
    private final UnsafeMemory memory;
    private final int offset;
    private final int capacity;
    /**
     * size actual actualUsed
     */
    private volatile int size;

    public MemoryBuffer(UnsafeMemory memory, int offset, int capacity) {
        this.memory = memory;
        this.offset = offset;
        this.capacity = capacity;
    }

    /**
     * write data.
     */
    public void write(byte[] data) {
        if (isDispose()) {
            throw new IllegalStateException("memory has been disposed");
        }
        if (data.length > this.capacity) {
            throw new BufferOverflowException();
        }
        this.size = data.length;
        memory.write(this.offset, data);
    }

    /**
     * read all data has been written in.
     */
    public byte[] read() {
        if (isDispose()) {
            throw new IllegalStateException("memory has been disposed");
        }
        byte[] buf = new byte[this.size];
        memory.read(this.offset, buf);
        return buf;
    }

    /**
     * read all data has been written in into buf.
     * we should reuse buf.
     */
    public byte[] read(byte[] buf) {
        if (isDispose()) {
            throw new IllegalStateException("memory has been disposed");
        }
        if (buf.length < this.size) {
            throw new BufferOverflowException();
        }
        memory.read(this.offset, buf, this.size);
        return buf;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public int getSize() {
        return this.size;
    }

    public int getOffset() {
        return this.offset;
    }

    public UnsafeMemory getMemory() {
        return this.memory;
    }

    /**
     * mark this buffer as destroyed.
     */
    public void dispose() {
        this.size = -1;
    }

    public boolean isDispose() {
        return this.size == -1;
    }
}
