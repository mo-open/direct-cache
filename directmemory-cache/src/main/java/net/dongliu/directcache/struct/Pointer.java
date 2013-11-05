package net.dongliu.directcache.struct;

import net.dongliu.directcache.memory.Allocator;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * Wrapper the memoryBuffer, and provide info-fields a cache entry needed.
 *
 * @author dongliu
 */
public class Pointer {

    private Object key;

    public final MemoryBuffer memoryBuffer;

    /**
     * version of the element. System.currentTimeMillis() is used to compute version for updated elements. That
     * way, the actual version of the updated element does not need to be checked.
     */
    private volatile long version;

    /**
     * The number of times the element was hit.
     */
    private volatile long hitCount;

    /**
     * The amount of time for the element to live, in seconds. 0 indicates unlimited.
     */
    private volatile int timeToLive = Integer.MIN_VALUE;

    /**
     * The amount of time for the element to idle, in seconds. 0 indicates unlimited.
     */
    private volatile int timeToIdle = Integer.MIN_VALUE;


    /**
     * If there is an Element in the Cache and it is replaced with a new Element for the same key,
     * then both the version number and lastUpdateTime should be updated to reflect that. The creation time
     * will be the creation time of the new Element, not the original one, so that TTL concepts still work.
     */
    private volatile long lastUpdateTime;

    private final AtomicBoolean live = new AtomicBoolean(true);


    public Pointer(MemoryBuffer memoryBuffer) {
        this.memoryBuffer = memoryBuffer;
        this.version = System.currentTimeMillis();
        this.lastUpdateTime = this.version;
        hitCount = 0;
    }

    public int getCapacity() {
        return memoryBuffer.getCapacity();
    }

    @Override
    public String toString() {
        return format("Pointer: %s[%s]", getClass().getSimpleName(), getSize());
    }

    public boolean isExpired() {
        long cur = System.currentTimeMillis();
        return timeToLive > 0 && cur - version > timeToLive || timeToIdle > 0 && cur - lastUpdateTime > timeToIdle;
    }

    public int getSize() {
        return memoryBuffer.getSize();
    }

    public void hit() {
        ++this.hitCount;
    }

    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

    public int getTimeToLive() {
        return this.timeToLive;
    }

    public void setTimeToIdle(int timeToIdle) {
        this.timeToIdle = timeToIdle;
    }

    public int getTimeToIdle() {
        return this.timeToIdle;
    }

    public MemoryBuffer getMemoryBuffer() {
        return memoryBuffer;
    }

    public AtomicBoolean getLive() {
        return live;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getVersion() {
        return this.version;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    public long getHitCount() {
        return this.hitCount;
    }

    public void returnTo(final Allocator allocator) {
        if (this.live.compareAndSet(true, false)) {
            allocator.free(this.memoryBuffer);
        }
    }

    public byte[] readValue() {
        return memoryBuffer.read();
    }
}
