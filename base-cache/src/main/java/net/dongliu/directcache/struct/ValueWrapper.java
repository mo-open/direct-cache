package net.dongliu.directcache.struct;

import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.utils.U;

/**
 * Wrapper the memoryBuffer, and provide info-fields a cache entry needed.
 *
 * @author dongliu
 */
public class ValueWrapper {

    private Object key;

    public final MemoryBuffer memoryBuffer;

    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int expiry = 0;

    /**
     * If there is an Element in the Cache and it is replaced with a new Element for the same key,
     * then lastUpdateTime should be updated to reflect that.
     */
    private volatile long lastUpdateTime;

    private volatile int live = 1;


    private static final long LIVE_OFFSET;

    static {
        LIVE_OFFSET = U.objectFieldOffset(ValueWrapper.class, "live");
    }

    public ValueWrapper(MemoryBuffer memoryBuffer) {
        this.memoryBuffer = memoryBuffer;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public int getCapacity() {
        return memoryBuffer.getCapacity();
    }

    public boolean isExpired() {
        long cur = System.currentTimeMillis();
        return expiry > 0 && cur - lastUpdateTime > expiry;
    }

    public int getSize() {
        return memoryBuffer.getSize();
    }

    public MemoryBuffer getMemoryBuffer() {
        return memoryBuffer;
    }

    public boolean isLive() {
        return live == 1;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    /**
     * return buffer to allocator.
     * @return false if pointer has already been freed.
     */
    public boolean returnTo(final Allocator allocator) {
        if (U.compareAndSwapInt(this, LIVE_OFFSET, 1, 0)) {
            allocator.free(this.memoryBuffer);
            return true;
        } else {
            return false;
        }
    }

    public byte[] readValue() {
        return memoryBuffer.read();
    }

    public int getExpiry() {
        return this.expiry;
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }
}
