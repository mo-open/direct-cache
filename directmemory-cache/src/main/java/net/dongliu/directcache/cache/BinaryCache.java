package net.dongliu.directcache.cache;

import net.dongliu.directcache.exception.TooLargeDataException;
import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.struct.MemoryBuffer;
import net.dongliu.directcache.struct.ValueWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A cache store binary values.The base for all caches.
 *
 * @author dongliu
 */
public class BinaryCache {

    private static final Logger logger = LoggerFactory.getLogger(BinaryCache.class);

    private CacheHashMap map;

    private final Allocator allocator;

    /**
     * Constructor
     */
    public BinaryCache(Allocator allocator) {
        //TODO: add cache builder to set parameters.
        this.allocator = allocator;
        this.map = new CacheHashMap(allocator, 1000, 0.75f, 256, null);
    }

    public void set(Object key, byte[] payload) {
        set(key, payload, 0);
    }

    /**
     * Put an element in the store if no element is currently mapped to the elements key.
     * @return the ValueWrapper previously cached for this key, or null if none.
     */
    public byte[] setIfAbsent(Object key, byte[] payload) {
        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            ValueWrapper oldValueWrapper = null;
            ValueWrapper valueWrapper = store(key, payload);
            if (valueWrapper != null) {
                oldValueWrapper = map.putIfAbsent(key, valueWrapper);
                //TODO: we need read node, but oldValueWrapper is already freed.
                //byte[] oldValue = oldValueWrapper.readValue();
                return null;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * whether the key exists.
     *
     * @return true if key exists.if key is expired still return true.
     */
    public boolean exists(Object key) {
        return this.map.containsKey(key);
    }

    /**
     * return all keys cached.
     */
    public Collection<Object> keys() {
        return this.map.keySet();
    }

    /**
     * store the node, return pointer.
     *
     * @return the old pointer, null if no old node.
     */
    public void set(Object key, byte[] payload, int expiresIn) {
        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            ValueWrapper oldValueWrapper = null;
            ValueWrapper valueWrapper = store(key, payload);
            if (valueWrapper != null) {
                if (expiresIn != 0) {
                    valueWrapper.setExpiry(expiresIn);
                }
                oldValueWrapper = map.put(key, valueWrapper);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * retrive node by key from cache.
     */
    public byte[] get(Object key) {
        ValueWrapper valueWrapper = retrievePointer(key);
        if (valueWrapper == null) {
            return null;
        }
        return valueWrapper.readValue();
    }

    /**
     * retrive node by key from cache.
     */
    private ValueWrapper retrievePointer(Object key) {
        ValueWrapper valueWrapper = map.get(key);
        if (valueWrapper == null) {
            return null;
        }

        if (!valueWrapper.isExpired() && valueWrapper.isLive()) {
            return valueWrapper;
        }

        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            valueWrapper = map.get(key);
            if (valueWrapper.isExpired() || !valueWrapper.isLive()) {
                map.remove(key);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    public void clear() {
        map.clear();
        logger.info("Cache cleared");
    }

    public void dispose() {
        map.clear();
        this.allocator.dispose();
        logger.info("Cache closed");
    }

    public long entries() {
        return map.size();
    }

    public void remove(Object key) {
        this.map.remove(key);
    }

    /**
     * allocate memory and store the payload, return the pointer.
     * @return the point.null if failed.
     */
    private ValueWrapper store(Object key, byte[] payload) {
        MemoryBuffer buffer;
        try {
            buffer = allocator.allocate(payload.length);
        } catch (TooLargeDataException e) {
            throw e;
        }

        // try to evict caches.
        if (buffer == null) {
            map.evict(key);
            buffer = allocator.allocate(payload.length);
        }

        if (buffer == null) {
            return null;
        }

        ValueWrapper p = new ValueWrapper(buffer);
        buffer.write(payload);
        p.setKey(key);
        return p;
    }

    private Lock getWriteLock(Object key) {
        return map.lockFor(key).writeLock();
    }

    public long used() {
        return this.allocator.used();
    }

    public ReentrantReadWriteLock.WriteLock lockFor(Object key) {
        return map.lockFor(key).writeLock();
    }
}
