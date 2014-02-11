package net.dongliu.direct.struct;

import net.dongliu.direct.memory.MemoryBuffer;

/**
 * basic valuewrapper, for base binary cache.
 *
 * @author dongliu
 */
public class BaseValueHolder extends BufferValueHolder {
    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int expiry = 0;

    /**
     * If there is an Element in the Cache and it is replaced with a new Element for the same key,
     * then lastUpdate should be updated to reflect that.
     */
    private volatile long lastUpdate;

    public BaseValueHolder(MemoryBuffer memoryBuffer) {
        super(memoryBuffer);
        this.lastUpdate = System.currentTimeMillis();
    }

    @Override
    public boolean isExpired() {
        long cur = System.currentTimeMillis();
        return expiry > 0 && cur - lastUpdate > expiry;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public long getLastUpdate() {
        return this.lastUpdate;
    }

    public int getExpiry() {
        return this.expiry;
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }

}
