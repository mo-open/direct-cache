package net.dongliu.direct;

import net.dongliu.direct.allocator.Allocator;
import net.dongliu.direct.allocator.ByteBuf;
import net.dongliu.direct.exception.CacheException;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import net.dongliu.direct.struct.BytesValue;
import net.dongliu.direct.struct.DirectValue;
import net.dongliu.direct.struct.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

    private final Serializer serializer;

    private static final int MAX_EVICTION_RATIO = 10;
    private static final int DEFAULT_SAMPLE_SIZE = 30;

    public static <S, T> DirectCacheBuilder<S, T> newBuilder() {
        return new DirectCacheBuilder<>();
    }

    /**
     * Constructor
     *
     * @param maxMemory the max off-heap size could use.
     */
    DirectCache(long maxMemory, int concurrency, Serializer serializer) {
        this.allocator = new Allocator(maxMemory,
                Runtime.getRuntime().availableProcessors() * 2);
        this.map = new ConcurrentMap(1024, 0.75f, concurrency);
        this.serializer = serializer;
    }

    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists.
     */
    public Value<V> get(K key, Class<V> clazz) {
        BytesValue<V> value = _get(key);

        if (value == null) {
            return null;
        }

        if (value.getBytes() == null) {
            return new Value<>(null);
        }

        try (InputStream in = new ByteArrayInputStream(value.getBytes())) {
            V v = serializer.deSerialize(in, clazz);
            return new Value<>(v);
        } catch (DeSerializeException | IOException e) {
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
            DirectValue holder = map.get(key);
            if (holder == null) {
                return null;
            }
            if (!holder.expired()) {
                byte[] bytes = holder.readValue();
                value = new BytesValue<>(bytes);
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
        _set(key, bytes, expiry);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  the value
     */
    private void _set(K key, byte[] value, int expiry) {
        DirectValue holder = store(key, value);
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

        return _add(key, bytes, expiry);
    }


    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    private boolean _add(K key, byte[] value, int expiry) {
        DirectValue oldDirectValue = map.get(key);
        if (oldDirectValue != null && !oldDirectValue.expired()) {
            return false;
        }
        DirectValue holder = store(key, value);

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

    private DirectValue store(K key, byte[] bytes) {
        ByteBuf buffer;
        buffer = this.allocator.newBuffer(bytes);
        if (buffer == null) {
            // cannot allocate memory, evict and try again
            lruEvict(key);
            buffer = this.allocator.newBuffer(bytes);
        }
        if (buffer == null) {
            return null;
        }

        return new DirectValue(allocator, buffer, key);
    }

    /**
     * return the actualUsed off-heap memory in bytes.
     */
    public long offHeapSize() {
        return this.allocator.used();
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

        DirectValue holder = findEvictionCandidate(key);
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

    public void destroy() {
        map.clear();
    }
}
