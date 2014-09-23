package net.dongliu.direct.memory;

import java.nio.BufferOverflowException;

/**
 * a direct-memory area.
 * this is not thread-safe
 *
 * @author Dong Liu
 */
public abstract class MemoryBuffer {

    /**
     * size actual used
     */
    private int size;

    protected MemoryBuffer() {
    }

    /**
     * write data.
     */
    public void write(byte[] data) {
        if (data.length > capacity()) {
            throw new BufferOverflowException();
        }
        this.size = data.length;
        getMemory().write(getOffset(), data);
    }

    /**
     * read all data has been written in.
     */
    public byte[] toBytes() {
        byte[] buf = new byte[this.size];
        getMemory().read(getOffset(), buf);
        return buf;
    }

    public abstract int capacity();

    public int size() {
        return this.size;
    }

    public abstract int getOffset();

    public abstract UnsafeMemory getMemory();

    /**
     * mark this buffer as destroyed.
     */
    public abstract void dispose();

}
