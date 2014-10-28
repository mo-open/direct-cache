package net.dongliu.direct.allocator;

/**
 * @author Dong Liu
 */
public abstract class AbstractBuffer {

    /**
     * release buffers
     */
    public abstract void release();

    public abstract int capacity();

    public abstract int size();

    public abstract byte[] toBytes();

    public abstract void write(byte[] bytes);
}
