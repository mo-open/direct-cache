package net.dongliu.direct.allocator;

/**
 * @author Dong Liu
 */
public class NullMemoryBuffer extends AbstractBuffer {

    private static NullMemoryBuffer instance = new NullMemoryBuffer();

    public static NullMemoryBuffer getInstance() {
        return instance;
    }

    @Override
    public void release() {

    }

    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public byte[] toBytes() {
        return null;
    }

    @Override
    public void write(byte[] bytes) {
        throw new UnsupportedOperationException("Unsupported for null");
    }
}