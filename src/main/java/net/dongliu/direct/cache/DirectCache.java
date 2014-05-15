package net.dongliu.direct.cache;

import net.dongliu.direct.exception.CacheException;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.serialization.ValueSerializer;
import net.dongliu.direct.struct.ValueHolder;
import net.dongliu.direct.utils.CacheConfigure;
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

    /**
     * Constructor
     *
     * @param maxSize the max off-heap size could use.
     */
    public DirectCache(int maxSize) {
        this(null, maxSize);
    }

    /**
     * Constructor
     */
    public DirectCache(CacheEventListener cacheEventListener, int maxSize) {
        this.allocator = SlabsAllocator.newInstance(maxSize);
        CacheConfigure cc = CacheConfigure.getConfigure();
        this.map = new ConcurrentMap(cc.getInitialSize(), cc.getLoadFactor(),
                cc.getConcurrency(), cacheEventListener);
    }

    /**
     * retrieve node by key from cache.
     */
    public <T> T get(Object key, ValueSerializer<T> deSerializer) {
        ValueHolder holder = retrieve(key);
        if (holder == null) {
            return null;
        }

        try {
            return readValue(holder, deSerializer);
        } finally {
            holder.dispose();
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
                holder.setExpiry(expiresIn);
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

        ValueHolder oldValueHolder = retrieve(key);
        if (oldValueHolder != null) {
            return false;
        }
        ValueHolder valueHolder = store(key, value, serializer);
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
                valueHolder.dispose();
            }
        }
    }

    /**
     * Put an element in the store only if the element is currently in cache.
     *
     * @return the previous value, if key is in cache(even if put op is failed), null otherwise.
     */
    public <T> T replace(Object key, T value, ValueSerializer<T> serializer) {
        ValueHolder oldHolder = retrieve(key);
        if (oldHolder == null) {
            return null;
        }
        ValueHolder holder = store(key, value, serializer);
        boolean needRelease = true;
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            oldHolder = retrieve(key);
            if (oldHolder == null) {
                return null;
            } else {
                needRelease = false;
                T oldValue;
                // TODO: move deSerialization out of locks
                try {
                    oldValue = readValue(oldHolder, serializer);
                } finally {
                    oldHolder.dispose();
                }
                if (holder != null) {
                    map.put(key, holder);
                } else {
                    //TODO: notify evict
                    map.remove(key);
                }
                return oldValue;
            }
        } finally {
            if (needRelease) {
                holder.dispose();
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
        } finally {
            lock.readLock().unlock();
        }

        if (!holder.isExpired()) {
            return holder;
        }

        holder.dispose();
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
            throw new CacheException(e);
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


    private <T> T readValue(ValueHolder holder, ValueSerializer<T> serializer) {
        try {
            return serializer.deserialize(holder.getMemoryBuffer().toBytes());
        } catch (DeSerializeException e) {
            throw new CacheException(e);
        }
    }

}
