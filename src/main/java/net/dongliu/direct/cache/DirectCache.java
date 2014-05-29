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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRU, direct-memory cache. both key and value cannot be null.
 * Both key and value cannot be null
 *
 * @author dongliu
 */
public class DirectCache {

    private static final Logger logger = LoggerFactory.getLogger(DirectCache.class);

    private ConcurrentMap map;

    private final Allocator allocator;

    private static final int MAX_EVICTION_RATIO = 10;

    private static final int DEFAULT_SAMPLE_SIZE = 30;


    public static DirectCacheBuilder newBuilder() {
        return new DirectCacheBuilder();
    }

    /**
     * Constructor
     *
     * @param maxSize the max off-heap size could use.
     */
    DirectCache(long maxSize, float expandFactor, int chunkSize, int slabSize,
                int initialSize, float loadFactor, int concurrency) {
        this.allocator = new SlabsAllocator(maxSize, expandFactor, chunkSize, slabSize);
        this.map = new ConcurrentMap(initialSize, loadFactor, concurrency);
    }

    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists
     */
    public <T> T get(Object key, ValueSerializer<T> serializer) {
        byte[] bytes = get(key);

        if (bytes == null) {
            return null;
        }

        try {
            return serializer.deserialize(bytes);
        } catch (DeSerializeException e) {
            throw new CacheException("deserialize value failed", e);
        }
    }

    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists
     */
    public byte[] get(Object key) {
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

        if (bytes == null) {
            //we cannot call remove expired item in read lock, so we remove it here
            removeExpiredEntry(key);
        }
        return bytes;
    }


    /**
     * set a value.if already exist, replace it
     *
     * @param value cannot be null
     */
    public <T> void set(Object key, T value, ValueSerializer<T> serializer) {
        set(key, value, serializer, 0);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  cannot be null
     */
    public <T> void set(Object key, T value, ValueSerializer<T> serializer, int expiry) {
        byte[] bytes;
        try {
            bytes = serializer.serialize(value);
        } catch (SerializeException e) {
            throw new CacheException("serialize value failed", e);
        }
        set(key, bytes, expiry);
    }

    /**
     * set a value.if already exist, replace it
     *
     * @param value cannot be null
     */
    public void set(Object key, byte[] value) {
        set(key, value, 0);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  cannot be null
     */
    public void set(Object key, byte[] value, int expiry) {
        ValueHolder holder = store(key, value);
        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            if (holder != null) {
                if (expiry > 0) {
                    holder.expiry(expiry);
                }
                map.put(key, holder);
            } else {
                // direct evict
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public <T> boolean add(Object key, T value, ValueSerializer<T> serializer) {
        return add(key, value, serializer, 0);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public <T> boolean add(Object key, T value, ValueSerializer<T> serializer, int expiry) {
        // we call map.get twice here, to avoid unnecessary serialize, not good
        ValueHolder oldValueHolder = map.get(key);
        if (oldValueHolder != null && !oldValueHolder.expired()) {
            return false;
        }

        byte[] bytes;
        try {
            bytes = serializer.serialize(value);
        } catch (SerializeException e) {
            throw new CacheException("serialize value failed", e);
        }

        return add(key, bytes, expiry);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    private boolean add(Object key, byte[] bytes) {
        return add(key, bytes, 0);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public boolean add(Object key, byte[] value, int expiry) {
        ValueHolder oldValueHolder = map.get(key);
        if (oldValueHolder != null && !oldValueHolder.expired()) {
            return false;
        }
        ValueHolder holder = store(key, value);

        ReentrantReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            // check again
            oldValueHolder = map.get(key);
            if (oldValueHolder != null && !oldValueHolder.expired()) {
                return false;
            }
            if (holder != null) {
                holder.expiry(expiry);
                oldValueHolder = map.putIfAbsent(key, holder);
            }
            return oldValueHolder == null;
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


    private ValueHolder store(Object key, byte[] bytes) {
        MemoryBuffer buffer = this.allocator.allocate(bytes.length);
        if (buffer == null) {
            // cannot allocate memory, evict and try again
            lruEvict(key);
            buffer = this.allocator.allocate(bytes.length);
        }
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


    /**
     * If the store is over capacity, evict elements until capacity is reached
     */
    private void lruEvict(Object key) {
        int evict = MAX_EVICTION_RATIO;
        if (allocator.getCapacity() < allocator.used()) {
            for (int i = 0; i < evict; i++) {
                removeChosenElements(key);
            }
        }
    }

    /**
     * Removes the element chosen by the eviction policy
     */
    private void removeChosenElements(Object key) {

        ValueHolder holder = findEvictionCandidate(key);
        if (holder == null) {
            logger.debug("Eviction selection miss. Selected element is null");
            return;
        }

        // If the element is expired, remove
        if (holder.expired()) {
            remove(holder.getKey());
            notifyExpiry(holder);
        } else {
            remove(holder);
        }
    }

    /**
     * Find a "relatively" unused element.
     *
     * @param key the element added by the action calling this check
     * @return the element chosen as candidate for eviction
     */
    private ValueHolder findEvictionCandidate(final Object key) {
        ValueHolder[] holders = sampleElements(key);
        if (holders.length == 0) {
            return null;
        }
        Arrays.sort(holders, new Comparator<ValueHolder>() {
            @Override
            public int compare(ValueHolder o1, ValueHolder o2) {
                if (o1.expired() && o2.expired()) {
                    return 0;
                } else if (o1.expired()) {
                    return -1;
                } else if (o2.expired()) {
                    return 1;
                } else {
                    return (int) (o1.lastUpdate() - o2.lastUpdate());
                }
            }
        });
        return holders[0];
    }

    /**
     * Uses random numbers to sample the entire map.
     * <p/>
     * This implementation uses a key array.
     *
     * @param keyHint a key used as a hint indicating where the just added element is
     * @return a random sample of elements
     */
    private ValueHolder[] sampleElements(Object keyHint) {
        int size = Math.min(map.quickSize(), DEFAULT_SAMPLE_SIZE);
        return map.getRandomValues(size, keyHint);
    }

    /**
     * Before eviction elements are checked.
     *
     * @param holder the element to notify about its expiry
     */
    private void notifyExpiry(final ValueHolder holder) {
        //to be implemented
    }

    private ReentrantReadWriteLock lockFor(Object key) {
        return map.lockFor(key);
    }

}
