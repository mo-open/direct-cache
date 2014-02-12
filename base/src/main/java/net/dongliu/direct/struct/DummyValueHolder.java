package net.dongliu.direct.struct;

/**
 * value hoder to hold nulls and empty objects.
 *
 * @author dongliu
 */
public abstract class DummyValueHolder implements ValueHolder {
    private final byte[] value;
    private Object key;

    protected DummyValueHolder(byte[] value) {
        this.value = value;
    }

    @Override
    public int getCapacity() {
        return 0;
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public boolean isLive() {
        return true;
    }

    @Override
    public Object getKey() {
        return this.key;
    }

    @Override
    public void setKey(Object key) {
        this.key = key;
    }

    @Override
    public byte[] readValue() {
        return this.value;
    }

    @Override
    public void release() {
    }

    @Override
    public void acquire() {
    }
}
