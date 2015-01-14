package net.dongliu.direct.value;

import net.dongliu.direct.allocator.ByteBuf;

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
    public final ByteBuf buffer;

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

    public DirectValue(ByteBuf buffer, Object key) {
        this.buffer = buffer;
        this.created = System.currentTimeMillis();
        this.key = key;
    }

    public int capacity() {
        return buffer.capacity();
    }

    public int size() {
        //TODO: get size
        return buffer.capacity();
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
}
