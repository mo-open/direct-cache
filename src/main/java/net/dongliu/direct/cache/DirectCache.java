package net.dongliu.direct.cache;

import net.dongliu.direct.exception.CacheException;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.serialization.ValueSerializer;
import net.dongliu.direct.struct.ValueHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRU, direct-memory cache. both key and value cannot be null.
 *
 * @author dongliu
 */
public class DirectCache {

    private static final Logger logger = LoggerFactory.getLogger(DirectCache.class);

    private ConcurrentMap map;

    private final Allocator allocator;


    public static DirectCacheBuilder newBuilder() {
        return new DirectCacheBuilder();
    }

    /**
     * Constructor
     *
     * @param maxSize the max off-heap size could use.
     */
    protected DirectCache(long maxSize, float expandFactor, int chunkSize, int slabSize,
                          int initialSize, float loadFactor, int concurrency,
                          CacheEventListener cacheEventListener) {
        this.allocator = new SlabsAllocator(maxSize, expandFactor, chunkSize, slabSize);
        this.map = new ConcurrentMap(initialSize, loadFactor, concurrency, cacheEventListener);
    }

    /**
     * retrieve node by key from cache.
     */
    public <T> T get(Object key, ValueSerializer<T> serializer) {
        ReentrantReadWriteLock lock = lockFor(key);
        lock.readLock().lock();
        byte[] bytes = null;
        try {
            ValueHolder holder = map.get(key);
            if (holder == null) {
                return null;
            }
            if (!holder.expired()) {
                bytes = holder.readValue();
            }
        } finally {
            lock.readLock().unlock();
        }

        if (bytes != null) {
            try {
                return serializer.deserialize(bytes);
            } catch (DeSerializeException e) {
                throw new CacheException("deserialize value failed", e);
            }
        } else {
            // expired
            removeExpiredEntry(key);
            return null;
        }
    }

    /**
     * set a value.
     *
     * @param value cannot be null
     */
    public <T> void set(Object key, T value, ValueSerializer<T> serializer, int expiresIn) {
        ValueHolder holder = store(key, value, serializer);
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            if (expiresIn > 0) {
                holder.expiry(expiresIn);
            }
            map.put(key, holder);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * set a value.
     *
     * @param value cannot be null
     */
    public <T> void set(Object key, T value, ValueSerializer<T> serializer) {
        set(key, value, serializer, 0);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public <T> boolean add(Object key, T value, ValueSerializer<T> serializer) {
        ValueHolder oldValueHolder = map.get(key);
        if (oldValueHolder != null && !oldValueHolder.expired()) {
            return false;
        }
        ValueHolder valueHolder = store(key, value, serializer);

        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            // check again
            oldValueHolder = map.get(key);
            if (oldValueHolder != null && !oldValueHolder.expired()) {
                return false;
            }
            oldValueHolder = map.putIfAbsent(key, valueHolder);
            return oldValueHolder == null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Put an element in the store only if the element is currently in cache.
     *
     * @return the previous value, if key is in cache(even if put op is failed), null otherwise.
     */
    public <T> T replace(Object key, T value, ValueSerializer<T> serializer) {
        ValueHolder oldHolder = map.get(key);
        if (oldHolder == null) {
            return null;
        }
        if (oldHolder.expired()) {
            removeExpiredEntry(key);
            return null;
        }

        ValueHolder holder = store(key, value, serializer);
        byte[] bytes = null;
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            oldHolder = map.get(key);
            if (oldHolder == null) {
                return null;
            }
            if (oldHolder.expired()) {
                removeExpiredEntry(key);
                return null;
            }

            bytes = oldHolder.readValue();
            if (holder != null) {
                map.put(key, holder);
            } else {
                //TODO: notify evict
                map.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
        try {
            return serializer.deserialize(bytes);
        } catch (DeSerializeException e) {
            throw new CacheException(e);
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

    private void removeExpiredEntry(Object key) {
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            ValueHolder newHolder = map.get(key);
            if (newHolder != null && newHolder.expired()) {
                map.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    /**
     * destroy cache, dispose all resources.
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


    private <T> ValueHolder store(Object key, T value, ValueSerializer<T> serializer) {
        byte[] bytes;
        try {
            bytes = serializer.serialize(value);
        } catch (SerializeException e) {
            throw new CacheException("serialize value failed", e);
        }
        MemoryBuffer buffer = this.allocator.allocate(bytes.length);
        if (buffer == null) {
            return null;
        }
        buffer.write(bytes);
        ValueHolder holder = new ValueHolder(buffer);
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
