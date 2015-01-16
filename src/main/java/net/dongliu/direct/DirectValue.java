package net.dongliu.direct;

import net.dongliu.direct.allocator.ByteBuf;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * interface of cache-value holder.
 *
 * @author Dong Liu
 */
public class DirectValue {

    private final Object key;

    /**
     * the direct buffer to store value. null if value if null.
     */
    private final ByteBuf buffer;

    /**
     * the value's class
     */
    private final Class type;

    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int expiry = 0;

    /**
     * The created time of this cache entry.
     * If  an Element in the Cache is replaced with a new Element for the same key,
     * then created should be updated to reflect that.
     */
    private volatile long created;

    // for lru
    DirectValue successor;
    DirectValue precursor;
    private volatile long lastPromoted;

    private static final AtomicLongFieldUpdater<DirectValue> updater
            = AtomicLongFieldUpdater.newUpdater(DirectValue.class, "lastPromoted");

    public DirectValue(Object key, ByteBuf buffer, Class type) {
        this.buffer = buffer;
        this.type = type;
        this.created = System.currentTimeMillis();
        this.key = key;
    }

    public int capacity() {
        return buffer.capacity();
    }

    public int size() {
        return buffer.size();
    }

    public Object getKey() {
        return key;
    }

    /**
     * read value in bytes
     *
     * @return not null
     */
    public byte[] readValue() {
        if (buffer == null) {
            return null;
        }
        // this guard is not thread-safe
        byte[] bytes = new byte[size()];
        buffer.readBytes(bytes);
        return bytes;
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    public Class getType() {
        return type;
    }

    public void release() {
        if (buffer != null) {
            buffer.release();
        }
    }

    public boolean expired() {
        long cur = System.currentTimeMillis();
        return expiry > 0 && cur - created > expiry;
    }

    public void lastUpdate(long lastUpdate) {
        this.created = lastUpdate;
    }

    public long lastUpdate() {
        return this.created;
    }

    public int expiry() {
        return this.expiry;
    }

    public void expiry(int expiry) {
        this.expiry = expiry;
    }

    long getLastPromoted() {
        return lastPromoted;
    }

    boolean compareAndSetLastPromoted(long expect, long update) {
        return updater.compareAndSet(this, expect, update);
    }

    public void setLastPromoted(long lastPromoted) {
        this.lastPromoted = lastPromoted;
    }
}
