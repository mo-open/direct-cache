package net.dongliu.direct.memory;

import java.nio.BufferOverflowException;

/**
 * the memory buffer for null values
 *
 * @author Dong Liu
 */
public class NullMemoryBuffer extends MemoryBuffer {

    private static final NullMemoryBuffer instance = new NullMemoryBuffer();

    public static NullMemoryBuffer getInstance() {
        return instance;
    }

    /**
     * write data.
     */
    public void write(byte[] data) {
        throw new UnsupportedOperationException();
    }

    /**
     * read all data has been written in.
     */
    public byte[] toBytes() {
        return null;
    }

    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    protected UnsafeMemory getMemory() {
        return null;
    }

    @Override
    public void dispose() {

    }
}
