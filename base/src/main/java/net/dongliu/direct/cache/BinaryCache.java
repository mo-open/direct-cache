package net.dongliu.direct.cache;

import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.struct.BaseDummyValueHolder;
import net.dongliu.direct.struct.BaseValueHolder;
import net.dongliu.direct.struct.ValueHolder;
import net.dongliu.direct.utils.CacheConfigure;
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

    private CacheConcurrentHashMap map;

    private final Allocator allocator;

    /**
     * @param maxSize
     */
    public BinaryCache(int maxSize) {
        this(null, maxSize);
    }

    /**
     * Constructor
     */
    public BinaryCache(CacheEventListener cacheEventListener, int maxSize) {
        this.allocator = SlabsAllocator.newInstance(maxSize);
        CacheConfigure cc = CacheConfigure.getConfigure();
        this.map = new CacheConcurrentHashMap(cc.getInitialSize(), cc.getLoadFactor(),
                cc.getConcurrency(), cacheEventListener);
    }

    public void set(Object key, byte[] payload) {
        set(key, payload, 0);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @return the value previously cached for this key, or null if none.
     */
    public byte[] add(Object key, byte[] payload) {
        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            ValueHolder oldValueHolder = null;
            ValueHolder valueHolder = store(key, payload);
            if (valueHolder != null) {
                oldValueHolder = map.putIfAbsent(key, valueHolder);
                //TODO: we need read node, but oldValueHolder is already freed.
                //byte[] oldValue = oldValueHolder.readValue();
                return null;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * to see wether the key exists or not.if the entry is expired still return true.
     *
     * @return true if key exists.
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
            BaseValueHolder valueWrapper = store(key, payload);
            if (valueWrapper != null) {
                if (expiresIn != 0) {
                    valueWrapper.setExpiry(expiresIn);
                }
                map.put(key, valueWrapper);
            } else {
                logger.debug("set value failed, cannot allocat memory");
            }

        } finally {
            lock.unlock();
        }
    }

    /**
     * retrive node by key from cache.
     */
    public byte[] get(Object key) {
        ValueHolder valueHolder = retrievePointer(key);
        if (valueHolder == null) {
            return null;
        }
        return valueHolder.readValue();
    }

    /**
     * retrive node by key from cache.
     */
    private ValueHolder retrievePointer(Object key) {
        ValueHolder valueHolder = map.get(key);
        if (valueHolder == null) {
            return null;
        }

        if (!valueHolder.isExpired() && valueHolder.isLive()) {
            return valueHolder;
        }

        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            valueHolder = map.get(key);
            if (valueHolder.isExpired() || !valueHolder.isLive()) {
                map.remove(key);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    /**
     * destroy cache, release all resources.
     */
    public void destroy() {
        map.clear();
        this.allocator.destroy();
        logger.info("Cache closed");
    }

    /**
     * the num of cache entries.
     */
    public long size() {
        return map.quickSize();
    }

    public void remove(Object key) {
        this.map.remove(key);
    }

    /**
     * allocate memory and store the payload, return the pointer.
     *
     * @return the point.null if failed.
     */
    private BaseValueHolder store(Object key, byte[] payload) {

        BaseValueHolder wrapper;
        if (payload == null) {
            wrapper = BaseDummyValueHolder.newNullValueHolder();
        } else if (payload.length == 0) {
            wrapper = BaseDummyValueHolder.newEmptyValueHolder();
        } else {
            MemoryBuffer buffer = allocator.allocate(payload.length);
            // try to evict caches.
            if (buffer == null) {
                map.evict(key);
                buffer = allocator.allocate(payload.length);
            }

            if (buffer == null) {
                logger.debug("Cannot allocate buffer for new key:" + key.toString());
                return null;
            }

            wrapper = new BaseValueHolder(buffer);
            buffer.write(payload);
        }
        wrapper.setKey(key);
        return wrapper;
    }

    private Lock getWriteLock(Object key) {
        return map.lockFor(key).writeLock();
    }

    /**
     * return the actualUsed offheap memory in bytes.
     */
    public long offHeapSize() {
        return this.allocator.actualUsed();
    }

    public ReentrantReadWriteLock.WriteLock lockFor(Object key) {
        return map.lockFor(key).writeLock();
    }
}
