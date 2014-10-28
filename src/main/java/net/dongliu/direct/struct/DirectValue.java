package net.dongliu.direct.struct;

import net.dongliu.direct.allocator.AbstractBuffer;
import net.dongliu.direct.allocator.Buffer;

/**
 * interface of cache-value holder.
 *
 * @author Dong Liu
 */
public class DirectValue<K, V> {

    private final K key;

    /**
     * the direct buffer to store value. null if value if null.
     */
    public final AbstractBuffer buffer;

    /**
     * the class of value
     */
    private final Class<V> clazz;

    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int expiry = 0;

    /**
     * If there is an Element in the Cache and it is replaced with a new Element for the same key,
     * then lastUpdate should be updated to reflect that.
     */
    private volatile long lastUpdate;

    public DirectValue(AbstractBuffer buffer, K key, Class<V> clazz) {
        this.buffer = buffer;
        this.lastUpdate = System.currentTimeMillis();
        this.key = key;
        this.clazz = clazz;
    }

    public int capacity() {
        return buffer.capacity();
    }

    public int size() {
        return buffer.size();
    }

    public K getKey() {
        return key;
    }

    /**
     * read value in bytes
     *
     * @return not null
     */
    public byte[] readValue() {
        // this guard is not thread-safe
        return buffer.toBytes();
    }

    public void release() {
        this.buffer.release();
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

    public Class<V> getClazz() {
        return clazz;
    }
}
