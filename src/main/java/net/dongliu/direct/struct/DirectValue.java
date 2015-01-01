package net.dongliu.direct.struct;

import net.dongliu.direct.allocator.Allocator;
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

    private final Allocator allocator;
    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int expiry = 0;

    /**
     * If there is an Element in the Cache and it is replaced with a new Element for the same key,
     * then lastUpdate should be updated to reflect that.
     */
    private volatile long lastUpdate;

    public DirectValue(Allocator allocator, ByteBuf buffer, Object key) {
        this.buffer = buffer;
        this.lastUpdate = System.currentTimeMillis();
        this.key = key;
        this.allocator = allocator;
    }

    public int capacity() {
        return buffer.capacity();
    }

    public int size() {
        return buffer.writerIndex();
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
        if (buffer == allocator.nullBuf) {
            return null;
        }
        // this guard is not thread-safe
        byte[] bytes = new byte[buffer.writerIndex()];
        buffer.readBytes(bytes);
        return bytes;
    }

    public void release() {
        this.allocator.release(buffer);
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
}
