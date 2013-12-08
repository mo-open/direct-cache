package net.dongliu.directcache.struct;

import net.sf.ehcache.util.TimeUtil;

/**
 * Value wrapper for ehcache elements
 *
 * @author dongliu
 */
public class EhcacheValueWrapper extends AbstractValueWrapper {

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

    private volatile long creationTime;

    private volatile long lastAccessTime;

    private Class valueClass;

    public EhcacheValueWrapper(MemoryBuffer memoryBuffer) {
        super(memoryBuffer);
    }

    @Override
    public boolean isExpired() {
        if (!isLifespanSet() || isEternal()) {
            return false;
        }
        long now = System.currentTimeMillis();
        long expirationTime = getExpirationTime();

        return now > expirationTime;
    }

    /**
     * Returns the expiration time based on time to live. If this element also has a time to idle setting, the expiry
     * time will vary depending on whether the element is accessed.
     *
     * @return the time to expiration
     */
    private long getExpirationTime() {
        if (!isLifespanSet() || isEternal()) {
            return Long.MAX_VALUE;
        }
        long expirationTime = 0;
        long ttlExpiry = version + TimeUtil.toMillis(getTimeToLive());

        long mostRecentTime = Math.max(version, lastUpdateTime);
        long ttiExpiry = mostRecentTime + TimeUtil.toMillis(getTimeToIdle());

        if (getTimeToLive() != 0 && (getTimeToIdle() == 0 || lastUpdateTime == 0)) {
            expirationTime = ttlExpiry;
        } else if (getTimeToLive() == 0) {
            expirationTime = ttiExpiry;
        } else {
            expirationTime = Math.min(ttlExpiry, ttiExpiry);
        }
        return expirationTime;
    }

    /**
     * @return true if the element is eternal
     */
    public boolean isEternal() {
        return (0 == timeToIdle) && (0 == timeToLive);
    }

    /**
     * Whether any combination of eternal, TTL or TTI has been set.
     *
     * @return true if set.
     */
    public boolean isLifespanSet() {
        return this.timeToIdle != Integer.MIN_VALUE || this.timeToLive != Integer.MIN_VALUE;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getHitCount() {
        return hitCount;
    }

    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    public int getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

    public int getTimeToIdle() {
        return timeToIdle;
    }

    public void setTimeToIdle(int timeToIdle) {
        this.timeToIdle = timeToIdle;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public void setValueClass(Class valueClass) {
        this.valueClass = valueClass;
    }
}
