package net.dongliu.direct.struct;

import net.dongliu.direct.memory.MemoryBuffer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * interface of cache-value holder.
 *
 * @author dongliu
 */
public class ValueHolder {

    private Object key;

    public final MemoryBuffer memoryBuffer;

    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int expiry = 0;

    /**
     * If there is an Element in the Cache and it is replaced with a new Element for the same key,
     * then lastUpdate should be updated to reflect that.
     */
    private volatile long lastUpdate;

    public ValueHolder(MemoryBuffer memoryBuffer) {
        this.memoryBuffer = memoryBuffer;
        this.lastUpdate = System.currentTimeMillis();
    }

    public int getCapacity() {
        return memoryBuffer.getCapacity();
    }

    public int getSize() {
        return memoryBuffer.getSize();
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public byte[] readValue() {
        // this guard is not thread-safe
        return memoryBuffer.toBytes();
    }

    public void dispose() {
        this.memoryBuffer.dispose();
    }

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

    public MemoryBuffer getMemoryBuffer() {
        return this.memoryBuffer;
    }
}
