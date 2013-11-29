package net.dongliu.directcache.struct;

/**
 * basic valuewrapper, for binary cache.
 * @author dongliu
 */
public class BaseValueWrapper extends AbstractValueWrapper{
    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int expiry = 0;

    /**
     * If there is an Element in the Cache and it is replaced with a new Element for the same key,
     * then lastUpdateTime should be updated to reflect that.
     */
    private volatile long lastUpdateTime;

    public BaseValueWrapper(MemoryBuffer memoryBuffer) {
        super(memoryBuffer);
        this.lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public boolean isExpired() {
        long cur = System.currentTimeMillis();
        return expiry > 0 && cur - lastUpdateTime > expiry;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    public int getExpiry() {
        return this.expiry;
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }
}
