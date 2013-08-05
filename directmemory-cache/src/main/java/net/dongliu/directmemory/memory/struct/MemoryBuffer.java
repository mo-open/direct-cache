package net.dongliu.directmemory.memory.struct;

import net.dongliu.directmemory.memory.allocator.MergedMemory;

import java.nio.BufferOverflowException;

/**
* @author dongliu
*/
public class MemoryBuffer {
    private final MergedMemory memory;
    private final long start;
    private final int capacity;
    private int size;

    public MemoryBuffer(MergedMemory memory, long start, int size) {
        this.memory = memory;
        this.start = start;
        this.capacity = size;
    }

    /**
     * write data.
     */
    public void write(byte[] data) {
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
        return memory.read(this.start, this.size);
    }

    /**
     * read bytes.
     *
     */
    public byte[] read(int size) {
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
}
