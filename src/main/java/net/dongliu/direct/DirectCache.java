package net.dongliu.direct;

import net.dongliu.direct.allocator.Allocator;
import net.dongliu.direct.allocator.ByteBuf;
import net.dongliu.direct.allocator.ByteBufInputStream;
import net.dongliu.direct.exception.CacheException;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import net.dongliu.direct.utils.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRU, direct-memory cache. both key and value cannot be null.
 * Both key and value cannot be null
 *
 * @author Dong Liu
 */
public class DirectCache {

    private static final Logger logger = LoggerFactory.getLogger(DirectCache.class);

    private ConcurrentMap map;
    private final Allocator allocator;

    private final Serializer serializer;

    private static final int MAX_EVICTION_NUM = 10;

    public static DirectCacheBuilder newBuilder() {
        return new DirectCacheBuilder();
    }

    /**
     * Constructor
     *
     * @param maxMemory the max off-heap size could use.
     */
    DirectCache(long maxMemory, int concurrency, Serializer serializer) {
        int arenaNum = Runtime.getRuntime().availableProcessors() * 2;
        this.allocator = new Allocator(arenaNum, Size.Kb(8), 11, maxMemory);
        this.map = new ConcurrentMap(1024, 0.75f, concurrency);
        this.serializer = serializer;
    }

    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists.
     */
    public <V extends Serializable> Value<V> get(Object key) {
        Value<InputStream> value = _get(key);

        if (value == null) {
            return null;
        } else if (value.getValue() == null) {
            return new Value<>(null, value.getType());
        }

        try (InputStream in = value.getValue()) {
            V v = (V) serializer.deSerialize(in, value.getType());
            return new Value<>(v, value.getType());
        } catch (DeSerializeException | IOException e) {
            throw new CacheException("deSerialize value failed", e);
        }
    }

    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists
     */
    private <V extends Serializable> Value<InputStream> _get(Object key) {
        Value<InputStream> value = null;
        ReentrantReadWriteLock lock = lockFor(key);
        lock.readLock().lock();
        try {
            DirectValue directValue = map.get(key);
            if (directValue == null) {
                // not exist
                return null;
            }
            if (!directValue.expired()) {
                InputStream in = directValue.getBuffer() == null ?
                        null : new ByteBufInputStream(directValue.getBuffer());
                value = new Value<>(in, directValue.getType());
            }
        } finally {
            lock.readLock().unlock();
        }

        if (value == null) {
            //we cannot call removeExpiredEntry in read lock(write lock-guarded), so we remove it here
            removeExpiredEntry(key);
        }
        return value;
    }


    /**
     * set a value.if already exist, replace it
     *
     * @param value cannot be null
     */
    public <V extends Serializable> void set(Object key, V value) {
        set(key, value, 0);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  cannot be null
     */
    public <V extends Serializable> void set(Object key, V value, int expiry) {
        byte[] bytes;
        if (value == null) {
            bytes = null;
        } else {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                serializer.serialize(value, bos);
                bytes = bos.toByteArray();
            } catch (SerializeException | IOException e) {
                throw new CacheException("serialize value failed", e);
            }
        }
        _set(key, bytes, value == null ? null : value.getClass(), expiry);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  the value
     */
    private void _set(Object key, byte[] value, Class clazz, int expiry) {
        DirectValue holder = store(key, value, clazz);
        if (holder == null) {
            // direct evict
            logger.debug("memory exceed capacity, direct evict occurred, key: {}", key);
            return;
        }
        if (expiry > 0) {
            holder.expiry(expiry);
        }

        map.put(key, holder);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public <V extends Serializable> boolean add(Object key, V value) {
        return add(key, value, 0);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public <V extends Serializable> boolean add(Object key, V value, int expiry) {
        // we call map.get twice here, to avoid unnecessary serialize, not good
        DirectValue oldDirectValue = map.get(key);
        if (oldDirectValue != null && !oldDirectValue.expired()) {
            return false;
        }

        byte[] bytes;
        if (value == null) {
            bytes = null;
        } else {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                serializer.serialize(value, bos);
                bytes = bos.toByteArray();
            } catch (SerializeException | IOException e) {
                throw new CacheException("serialize value failed", e);
            }
        }

        return _add(key, bytes, value == null ? null : value.getClass(), expiry);
    }


    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    private boolean _add(Object key, byte[] value, Class clazz, int expiry) {
        DirectValue oldDirectValue = map.get(key);
        if (oldDirectValue != null && !oldDirectValue.expired()) {
            return false;
        }
        DirectValue holder = store(key, value, clazz);

        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            // check again
            oldDirectValue = map.get(key);
            if (oldDirectValue != null && !oldDirectValue.expired()) {
                return false;
            }
            if (holder != null) {
                holder.expiry(expiry);
                oldDirectValue = map.putIfAbsent(key, holder);
            }
            return oldDirectValue == null;
        } finally {
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
    public Collection<?> keys() {
        return this.map.keySet();
    }

    private void removeExpiredEntry(Object key) {
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            DirectValue newHolder = map.get(key);
            if (newHolder != null && newHolder.expired()) {
                map.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
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

    private DirectValue store(Object key, byte[] bytes, Class clazz) {
        if (bytes == null) {
            return new DirectValue(key, null, clazz);
        }

        ByteBuf buffer;
        buffer = this.allocator.directBuffer(bytes.length);
        if (buffer == null) {
            // cannot allocate memory, evict and try again
            evict(key);
            buffer = this.allocator.directBuffer(bytes.length);
        }
        if (buffer == null) {
            return null;
        }
        buffer.writeBytes(bytes);

        return new DirectValue(key, buffer, clazz);
    }

    /**
     * return the actualUsed off-heap memory in bytes.
     */
    public long offHeapSize() {
        return this.allocator.getUsed().get();
    }


    /**
     * If the store is over size, evict elements until size is reached
     */
    private void evict(Object key) {
        int evict = MAX_EVICTION_NUM;
        List<DirectValue> candidates = map.evictCandidates(key, evict);
        logger.debug("evict keys via lru, count: {}", candidates.size());
        for (DirectValue value : candidates) {
            removeChosenElements(value);
        }
    }

    /**
     * Removes the element chosen by the eviction policy
     */
    private void removeChosenElements(DirectValue directValue) {

        // If the element is expired, remove
        remove(directValue.getKey());
    }

    private ReentrantReadWriteLock lockFor(Object key) {
        return map.lockFor(key);
    }

    public void destroy() {
        map.clear();
    }
}
