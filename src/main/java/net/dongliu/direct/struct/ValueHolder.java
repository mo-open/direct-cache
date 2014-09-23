package net.dongliu.direct.struct;

import net.dongliu.direct.memory.MemoryBuffer;

/**
 * interface of cache-value holder.
 *
 * @author  Dong Liu
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

    public int capacity() {
        return memoryBuffer.capacity();
    }

    public int size() {
        return memoryBuffer.size();
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    /**
     * read value in bytes
     * @return not null
     */
    public byte[] readValue() {
        // this guard is not thread-safe
        return memoryBuffer.toBytes();
    }

    public void dispose() {
        this.memoryBuffer.dispose();
    }

    public boolean expired() {
        long cur = System.currentTimeMillis();
        return expiry > 0 && cur - lastUpdate > expiry;
    }

    public void lastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public long lastUpdate() {
        return this.lastUpdate;
    }

    public int expiry() {
        return this.expiry;
    }

    public void expiry(int expiry) {
        this.expiry = expiry;
    }

    public MemoryBuffer getMemoryBuffer() {
        return this.memoryBuffer;
    }
}
