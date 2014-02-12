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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A cache store binary values.The base for all caches.
 *
 * @author dongliu
 */
public class BaseDirectCache {

    private static final Logger logger = LoggerFactory.getLogger(BaseDirectCache.class);

    private CacheMap map;

    private final Allocator allocator;

    /**
     * Constructor
     *
     * @param maxSize the max offheap size could use.
     */
    public BaseDirectCache(int maxSize) {
        this(null, maxSize);
    }

    /**
     * Constructor
     */
    public BaseDirectCache(CacheEventListener cacheEventListener, int maxSize) {
        this.allocator = SlabsAllocator.newInstance(maxSize);
        CacheConfigure cc = CacheConfigure.getConfigure();
        this.map = new CacheMap(cc.getInitialSize(), cc.getLoadFactor(),
                cc.getConcurrency(), cacheEventListener);
    }

    /**
     * retrive node by key from cache.
     */
    public byte[] get(Object key) {
        ValueHolder valueHolder = retrieve(key);
        if (valueHolder == null) {
            return null;
        }

        try {
            return valueHolder.readValue();
        } finally {
            valueHolder.release();
        }
    }

    /**
     * store the node, return pointer.
     */
    public void set(Object key, byte[] payload, int expiresIn) {
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
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
            lock.writeLock().unlock();
        }
    }

    /**
     * set a value.
     */
    public void set(Object key, byte[] payload) {
        set(key, payload, 0);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public boolean add(Object key, byte[] payload) {
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            ValueHolder oldValueHolder = retrieve(key);
            if (oldValueHolder == null) {
                ValueHolder valueHolder = store(key, payload);
                if (valueHolder != null) {
                    map.put(key, valueHolder);
                }
                return true;
            } else {
                oldValueHolder.release();
                return false;
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Put an element in the store only if the element is currently in cache.
     *
     * @return true if the key is in cache(even if put op is failed), false otherwise.
     */
    public boolean replace(Object key, byte[] payload) {
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            ValueHolder oldValueHolder = retrieve(key);
            if (oldValueHolder != null) {
                oldValueHolder.release();
                ValueHolder valueHolder = store(key, payload);
                if (valueHolder != null) {
                    map.put(key, valueHolder);
                }
                return true;
            } else {
                return false;
            }
        } finally {
            lock.writeLock().unlock();
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
     * retrive node by key from cache.
     * NOTE: the value hoder return need to be released!
     */
    private ValueHolder retrieve(Object key) {
        ReentrantReadWriteLock lock = lockFor(key);

        lock.readLock().lock();
        ValueHolder valueHolder;
        try {
            valueHolder = map.get(key);
            if (valueHolder == null) {
                return null;
            }
            // make sure valueHolder is not disposed.
            valueHolder.acquire();
        } finally {
            lock.readLock().unlock();
        }

        if (!valueHolder.isExpired()) {
            return valueHolder;
        }

        lock.writeLock().lock();
        try {
            valueHolder = map.get(key);
            // judge again.
            if (valueHolder.isExpired()) {
                valueHolder.release();
                map.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return null;
    }

    /**
     * destroy cache, release all resources.
     */
    public void destroy() {
        map.clear();
        this.allocator.destroy();
        logger.debug("Cache closed");
    }

    /**
     * the num of cache entries.
     */
    public long size() {
        return map.quickSize();
    }

    /**
     * remove key from cache
     */
    public void remove(Object key) {
        this.map.remove(key);
    }

    /**
     * allocate memory and store the payload, return the pointer.
     *
     * @return the point.null if failed.
     */
    private BaseValueHolder store(Object key, byte[] payload) {

        BaseValueHolder holder;
        if (payload == null) {
            holder = BaseDummyValueHolder.newNullValueHolder();
        } else if (payload.length == 0) {
            holder = BaseDummyValueHolder.newEmptyValueHolder();
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

            holder = new BaseValueHolder(buffer);
            buffer.write(payload);
        }
        holder.setKey(key);
        return holder;
    }

    /**
     * return the actualUsed offheap memory in bytes.
     */
    public long offHeapSize() {
        return this.allocator.actualUsed();
    }

    private ReentrantReadWriteLock lockFor(Object key) {
        return map.lockFor(key);
    }
}
