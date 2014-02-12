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
     * @param maxSize the max off-heap size could use.
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
     * retrieve node by key from cache.
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
            BaseValueHolder holder = store(key, payload);
            if (holder != null) {
                if (expiresIn != 0) {
                    holder.setExpiry(expiresIn);
                }
                map.put(key, holder);
            } else {
                map.remove(key);
                //TODO: notify evict
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

        ValueHolder oldValueHolder = retrieve(key);
        if (oldValueHolder != null) {
            return false;
        }
        ValueHolder valueHolder = store(key, payload);
        if (valueHolder == null) {
            //TODO: notify evict
            return true;
        }
        boolean needRelease = true;

        try {
            oldValueHolder = map.putIfAbsent(key, valueHolder);
            if (oldValueHolder == null) {
                needRelease = false;
                return true;
            } else {
                return false;
            }
        } finally {
            if (needRelease) {
                valueHolder.release();
            }
        }
    }

    /**
     * Put an element in the store only if the element is currently in cache.
     *
     * @return the previous value, if key is in cache(even if put op is failed), null otherwise.
     */
    public byte[] replace(Object key, byte[] payload) {
        ValueHolder oldValueHolder = retrieve(key);
        if (oldValueHolder == null) {
            return null;
        }
        ValueHolder valueHolder = store(key, payload);
        boolean needRelease = true;
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            oldValueHolder = retrieve(key);
            if (oldValueHolder == null) {
                return null;
            } else {
                needRelease = false;
                byte[] value;
                try {
                    value = oldValueHolder.readValue();
                } finally {
                    oldValueHolder.release();
                }
                if (valueHolder != null) {
                    map.put(key, valueHolder);
                } else {
                    //TODO: notify evict
                    map.remove(key);
                }
                return value;
            }
        } finally {
            if (needRelease) {
                valueHolder.release();
            }
            lock.writeLock().unlock();
        }
    }

    /**
     * to see weather the key exists or not.if the entry is expired still return true.
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
     * retrieve node by key from cache.
     * NOTE: the value holder return need to be released!
     */
    private ValueHolder retrieve(Object key) {
        ReentrantReadWriteLock lock = lockFor(key);

        lock.readLock().lock();
        ValueHolder holder;
        try {
            holder = map.get(key);
            if (holder == null) {
                return null;
            }
            // make sure valueHolder is not disposed.
            holder.acquire();
        } finally {
            lock.readLock().unlock();
        }

        if (!holder.isExpired()) {
            return holder;
        }

        holder.release();
        lock.writeLock().lock();
        try {
            ValueHolder newHolder = map.get(key);
            if (newHolder != null && newHolder.isExpired()) {
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
                map.evictEntries(key);
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
     * return the actualUsed off-heap memory in bytes.
     */
    public long offHeapSize() {
        return this.allocator.actualUsed();
    }

    private ReentrantReadWriteLock lockFor(Object key) {
        return map.lockFor(key);
    }
}
