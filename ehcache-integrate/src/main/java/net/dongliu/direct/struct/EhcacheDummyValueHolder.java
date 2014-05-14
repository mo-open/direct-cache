package net.dongliu.direct.struct;

/**
 * Value wrapper for ehcache elements
 *
 * @author dongliu
 */
public class EhcacheDummyValueHolder extends EhcacheValueHolder {

    public final byte[] value;

    private EhcacheDummyValueHolder(byte[] value) {
        super(null);
        this.value = value;
    }

    public static EhcacheDummyValueHolder newNullValueHolder() {
        return new EhcacheDummyValueHolder(null);
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
