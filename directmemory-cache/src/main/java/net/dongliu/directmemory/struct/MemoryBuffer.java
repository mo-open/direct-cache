package net.dongliu.directmemory.struct;

import net.dongliu.directmemory.memory.MergedMemory;

import java.nio.BufferOverflowException;

/**
 * a memory area.
 * @author dongliu
 */
public class MemoryBuffer {
    private final MergedMemory memory;
    private final long start;
    private final int capacity;
    /** size actual used */
    private int size;

    public MemoryBuffer(MergedMemory memory, long start, int capacity) {
        this.memory = memory;
        this.start = start;
        this.capacity = capacity;
    }

    /**
     * write data.
     */
    public void write(byte[] data) {
        if (isDispose()) {
            throw new NullPointerException();
        }
        if (data.length > this.capacity) {
            throw new BufferOverflowException();
        }
        this.size = data.length;
        memory.write(this.start, data);
    }

    /**
     * read all data has been written in.
     */
    public byte[] read() {
        if (isDispose()) {
            throw new NullPointerException();
        }
        return memory.read(this.start, this.size);
    }

    /**
     * read bytes.
     */
    public byte[] read(int size) {
        if (isDispose()) {
            throw new NullPointerException();
        }
        if (size > this.size) {
            throw new BufferOverflowException();
        }
        return memory.read(this.start, size);
    }

    public int getCapacity() {
        return this.capacity;
    }

    public int getSize() {
        return this.size;
    }

    public long getStart() {
        return this.start;
    }

    public MergedMemory getMemory() {
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
