package net.dongliu.direct.memory;

import java.nio.BufferOverflowException;

/**
 * a memory area.
 *
 * @author dongliu
 */
public abstract class MemoryBuffer {

    /**
     * size actual actualUsed
     */
    private volatile int size;

    protected MemoryBuffer() {
    }

    /**
     * write data.
     */
    public void write(byte[] data) {
        if (data.length > getCapacity()) {
            throw new BufferOverflowException();
        }
        this.size = data.length;
        getMemory().write(getOffset(), data);
    }

    /**
     * read all data has been written in.
     */
    public byte[] read() {
        byte[] buf = new byte[this.size];
        getMemory().read(getOffset(), buf);
        return buf;
    }

    /**
     * read all data has been written in into buf.
     * we should reuse buf.
     */
    public byte[] read(byte[] buf) {
        if (buf.length < this.size) {
            throw new BufferOverflowException();
        }
        getMemory().read(getOffset(), buf, this.size);
        return buf;
    }

    public abstract int getCapacity();

    public int getSize() {
        return this.size;
    }

    public abstract int getOffset();

    public abstract UnsafeMemory getMemory();

    /**
     * mark this buffer as destroyed.
     */
    public abstract void dispose();

}
