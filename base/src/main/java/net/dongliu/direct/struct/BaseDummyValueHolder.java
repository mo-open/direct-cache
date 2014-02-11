package net.dongliu.direct.struct;

/**
 * value hoder to hold nulls and empty objects.
 *
 * @author dongliu
 */
public class BaseDummyValueHolder extends BaseValueHolder {

    public final byte[] value;

    private BaseDummyValueHolder(byte[] value) {
        super(null);
        this.value = value;
    }

    public static BaseDummyValueHolder newNullValueHolder() {
        return new BaseDummyValueHolder(null);
    }

    public static BaseDummyValueHolder newEmptyValueHolder() {
        return new BaseDummyValueHolder(new byte[0]);
    }

    @Override
    public int getCapacity() {
        return 0;
    }

    @Override
    public boolean isExpired() {
        return super.isExpired();
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public boolean isLive() {
        return super.isLive();
    }

    @Override
    public Object getKey() {
        return super.getKey();
    }

    @Override
    public void setKey(Object key) {
        super.setKey(key);
    }

    @Override
    public void dispose() {
        super.markDead();
    }

    @Override
    public byte[] readValue() {
        return this.value;
    }
}
