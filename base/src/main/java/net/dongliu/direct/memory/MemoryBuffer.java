package net.dongliu.direct.memory;

import java.nio.BufferOverflowException;

/**
 * a direct-memory area.
 * this is not thread-safe
 *
 * @author dongliu
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
        if (data.length > getCapacity()) {
            throw new BufferOverflowException();
        }
        this.size = data.length;
        getMemory().write(getOffset(), data);
    }

    /**
     * read all data has been written in.
     */
    public byte[] readAll() {
        byte[] buf = new byte[this.size];
        getMemory().read(getOffset(), buf);
        return buf;
    }

    /**
     * read data
     *
     * @param offset the offset of dest array
     * @param pos    the offset of this buffer to read
     * @param size   the size to read
     * @return size actually read.  -1 if no data available
     */
    public int read(byte[] buf, int offset, int pos, int size) {
        if (buf.length - offset < size) {
            throw new BufferOverflowException();
        }
        if (pos >= this.size) {
            return -1;
        }
        int readed = size;
        if (readed + pos > this.size) {
            readed = this.size - pos;
        }
        getMemory().read(getOffset() + pos, buf, offset, readed);
        return readed;
    }

    /**
     * read one byte at the offset of this buffer
     *
     * @return the byte value by int, -1 if no data available
     */
    public int read(int pos) {
        if (pos >= size) {
            return -1;
        }
        return getMemory().read(pos) & 0xff;
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
