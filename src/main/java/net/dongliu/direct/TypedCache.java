package net.dongliu.direct;

import java.util.Collection;

/**
 * Cache with type of key and value
 *
 * @author Dong Liu
 */
public class TypedCache<K, V> {

    private final DirectCache cache;
    private final Class<V> valueClass;
    private final Class<K> keyClass;

    /**
     * Constructor
     *
     * @param maxMemory the max off-heap size could use.
     */
    TypedCache(long maxMemory, int concurrency, Serializer serializer,
               Class<K> keyClass, Class<V> valueClass) {
        cache = new DirectCache(maxMemory, concurrency, serializer);
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }


    /**
     * retrieve node by key from cache.
     *
     * @return null if not exists.
     */
    public Value<V> get(K key) {
        return cache.get(key, valueClass);
    }

    /**
     * set a value.if already exist, replace it
     *
     * @param value cannot be null
     */
    public void set(K key, V value) {
        cache.set(key, value);
    }

    /**
     * set a value. if already exist, replace it
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @param value  cannot be null
     */
    public void set(K key, V value, int expiry) {
        cache.set(key, value, expiry);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public boolean add(K key, V value) {
        return cache.add(key, value);
    }

    /**
     * Put an element in the store only if no element is currently mapped to the elements key.
     *
     * @param expiry The amount of time for the element to live, in seconds.
     * @return true if the key is not in cache(even if put op is failed), false otherwise.
     */
    public boolean add(K key, V value, int expiry) {
        return cache.add(key, value, expiry);
    }

    /**
     * remove key from cache
     */
    public void remove(K key) {
        cache.remove(key);
    }

    /**
     * to see weather the key exists or not.if the entry is expired still return true.
     *
     * @return true if key exists.
     */
    public boolean exists(K key) {
        return cache.exists(key);
    }

    /**
     * return all keys cached.
     */
    public Collection<K> keys() {
        return (Collection<K>) cache.keys();
    }

    /**
     * the num of cache entries.
     */
    public long size() {
        return cache.size();
    }

    /**
     * return the actualUsed off-heap memory in bytes.
     */
    public long offHeapSize() {
        return cache.offHeapSize();
    }

    public void destroy() {
        cache.destroy();
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }
}
