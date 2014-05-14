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

    private final AtomicInteger count;

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
        this.count = new AtomicInteger(1);
        this.lastUpdate = System.currentTimeMillis();
    }

    public boolean isLive() {
        return this.count.intValue() > 0;
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
        if (count.intValue() <= 0) {
            throw new IllegalArgumentException("This buffer has been disposed.");
        }
        return memoryBuffer.toBytes();
    }

    public void release() {
        if (count.decrementAndGet() == 0) {
            this.memoryBuffer.dispose();
        }
    }

    public void acquire() {
        if (count.incrementAndGet() <= 1) {
            count.decrementAndGet();
            throw new IllegalArgumentException("This buffer has been disposed.");
        }
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

    public MemoryBuffer getMemoryBuffer (){
        return this.memoryBuffer;
    }
}
