package net.dongliu.direct.cache;

import net.dongliu.direct.exception.CacheException;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.NullMemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.serialization.Serializer;
import net.dongliu.direct.struct.BytesValue;
import net.dongliu.direct.struct.DirectValue;
import net.dongliu.direct.struct.Value;
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
 * @author Dong Liu
 */
public class DirectCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DirectCache.class);

    private ConcurrentMap map;

    private final Allocator allocator;

    private static final int MAX_EVICTION_RATIO = 10;

    private static final int DEFAULT_SAMPLE_SIZE = 30;

    private Serializer<V> serializer;

    public static <S, T> DirectCacheBuilder<S, T> newBuilder() {
        return new DirectCacheBuilder<>();
    }

    /**
     * Constructor
     *
     * @param maxSize the max off-heap size could use.
     */
    DirectCache(long maxSize, float expandFactor, int chunkSize, int slabSize,
                int initialSize, float loadFactor, int concurrency, Serializer<V> serializer) {
        this.allocator = new SlabsAllocator(maxSize, expandFactor, chunkSize, slabSize);
        this.map = new ConcurrentMap(initialSize, loadFactor, concurrency);
        this.serializer = serializer;
    }

    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists.
     */
    public Value<V> get(K key) {
        BytesValue<V> value = _get(key);

        if (value == null) {
            return null;
        }

        if (value.getBytes() == null) {
            return new Value<>(null);
        }

        try {
            V v = serializer.deSerialize(value.getBytes(), value.getClazz());
            return new Value<>(v);
        } catch (DeSerializeException e) {
            throw new CacheException("deSerialize value failed", e);
        }
    }

    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists
     */
    private BytesValue<V> _get(K key) {
        ReentrantReadWriteLock lock = lockFor(key);
        lock.readLock().lock();
        BytesValue<V> value = null;
        try {
            DirectValue<K, V> holder = map.get(key);
            if (holder == null) {
                return null;
            }
            if (!holder.expired()) {
                byte[] bytes = holder.readValue();
                value = new BytesValue<>(bytes, holder.getClazz());
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
    public void set(K key, V value) {
        set(key, value, 0);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  cannot be null
     */
    @SuppressWarnings("unchecked")
    public void set(K key, V value, int expiry) {
        byte[] bytes;
        Class<V> clazz;
        if (value == null) {
            bytes = null;
            clazz = null;
        } else {
            try {
                clazz = (Class<V>) value.getClass();
                bytes = serializer.serialize(value);
            } catch (SerializeException e) {
                throw new CacheException("serialize value failed", e);
            }
        }
        _set(key, bytes, clazz, expiry);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  the value
     * @param clazz  the class of value
     */
    private void _set(K key, byte[] value, Class<V> clazz, int expiry) {
        DirectValue holder = store(key, value, clazz);
        if (holder == null) {
            // direct evict
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
    public boolean add(K key, V value) {
        return add(key, value, 0);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean add(K key, V value, int expiry) {
        // we call map.get twice here, to avoid unnecessary serialize, not good
        DirectValue oldDirectValue = map.get(key);
        if (oldDirectValue != null && !oldDirectValue.expired()) {
            return false;
        }

        byte[] bytes;
        Class<V> clazz;
        if (value == null) {
            bytes = null;
            clazz = null;
        } else {
            try {
                clazz = (Class<V>) value.getClass();
                bytes = serializer.serialize(value);
            } catch (SerializeException e) {
                throw new CacheException("serialize value failed", e);
            }
        }

        return _add(key, bytes, clazz, expiry);
    }


    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    private boolean _add(K key, byte[] value, Class<V> clazz, int expiry) {
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
    public boolean exists(K key) {
        return this.map.containsKey(key);
    }

    /**
     * return all keys cached.
     */
    public Collection<K> keys() {
        return (Collection<K>) this.map.keySet();
    }

    private void removeExpiredEntry(K key) {
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
    public void remove(K key) {
        this.map.remove(key);
    }

    private DirectValue<K, V> store(K key, byte[] bytes, Class<V> clazz) {
        MemoryBuffer buffer;
        if (bytes == null) {
            buffer = NullMemoryBuffer.getInstance();
        } else {
            buffer = this.allocator.allocate(bytes.length);
            if (buffer == null) {
                // cannot allocate memory, evict and try again
                lruEvict(key);
                buffer = this.allocator.allocate(bytes.length);
            }
            if (buffer == null) {
                return null;
            }

            buffer.write(bytes);
        }
        return new DirectValue<>(buffer, key, clazz);
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
    private void lruEvict(K key) {
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
    private void removeChosenElements(K key) {

        DirectValue<K, V> holder = findEvictionCandidate(key);
        if (holder == null) {
            logger.debug("Eviction selection miss. Selected element is null");
            return;
        }

        // If the element is expired, remove
        if (holder.expired()) {
            remove(holder.getKey());
            notifyExpiry(holder);
        } else {
            remove(holder.getKey());
        }
    }

    /**
     * Find a "relatively" unused element.
     *
     * @param key the element added by the action calling this check
     * @return the element chosen as candidate for eviction
     */
    private DirectValue findEvictionCandidate(final Object key) {
        DirectValue[] holders = sampleElements(key);
        if (holders.length == 0) {
            return null;
        }
        Arrays.sort(holders, new Comparator<DirectValue>() {
            @Override
            public int compare(DirectValue o1, DirectValue o2) {
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
     * <p>
     * This implementation uses a key array.
     * </p>
     *
     * @param keyHint a key used as a hint indicating where the just added element is
     * @return a random sample of elements
     */
    private DirectValue[] sampleElements(Object keyHint) {
        int size = Math.min(map.quickSize(), DEFAULT_SAMPLE_SIZE);
        return map.getRandomValues(size, keyHint);
    }

    /**
     * Before eviction elements are checked.
     *
     * @param holder the element to notify about its expiry
     */
    private void notifyExpiry(final DirectValue holder) {
        //to be implemented
    }

    private ReentrantReadWriteLock lockFor(Object key) {
        return map.lockFor(key);
    }

}
