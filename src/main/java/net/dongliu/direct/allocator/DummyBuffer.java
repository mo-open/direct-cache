package net.dongliu.direct.allocator;

/**
 * dummy buffer for null values
 *
 * @author Dong Liu
 */
public class DummyBuffer extends AbstractBuffer {

    private static DummyBuffer instance = new DummyBuffer();

    public static DummyBuffer getInstance() {
        return instance;
    }

    private DummyBuffer() {
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