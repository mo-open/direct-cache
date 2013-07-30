package org.apache.directmemory.ehcache;
import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.memory.MemoryManagerService;
import org.apache.directmemory.memory.MemoryManagerServiceImpl;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

import java.util.Set;

import static org.apache.directmemory.DirectMemory.DEFAULT_CONCURRENCY_LEVEL;
import static org.apache.directmemory.DirectMemory.DEFAULT_INITIAL_CAPACITY;

/**
 * @param <K>
 * @param <V>
 * @author michaelandrepearce
 */
public class DirectMemoryCache<K, V> {

    private final CacheService<K, V> cacheService;

    public DirectMemoryCache(int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel) {
        MemoryManagerService<V> memoryManager =
                new MemoryManagerServiceImpl<V>(false);

        cacheService = new DirectMemory<K, V>().setMemoryManager(memoryManager)
                .setNumberOfBuffers(numberOfBuffers)
                .setSize(size)
                .setInitialCapacity(initialCapacity)
                .setConcurrencyLevel(concurrencyLevel)
                .newCacheService();
    }

    public DirectMemoryCache(int numberOfBuffers, int size) {
        this(numberOfBuffers, size, DEFAULT_INITIAL_CAPACITY, DEFAULT_CONCURRENCY_LEVEL);
    }

    public void scheduleDisposalEvery(long l) {
        cacheService.scheduleDisposalEvery(l);
    }

    public Pointer<V> putByteArray(K key, byte[] payload, int expiresIn) {
        return cacheService.putByteArray(key, payload, expiresIn);
    }

    public Pointer<V> putByteArray(K key, byte[] payload) {
        return cacheService.putByteArray(key, payload);
    }

    public Pointer<V> put(K key, V value) {
        return cacheService.put(key, value);
    }

    public Pointer<V> put(K key, V value, int expiresIn) {
        return cacheService.put(key, value, expiresIn);
    }

    public byte[] retrieveByteArray(K key) {
        return cacheService.retrieveByteArray(key);
    }

    public V retrieve(K key) {
        return cacheService.retrieve(key);
    }

    public Pointer<V> getPointer(K key) {
        return cacheService.getPointer(key);
    }

    public void free(K key) {
        cacheService.free(key);
    }

    public void free(Pointer<V> pointer) {
        cacheService.free(pointer);
    }

    public void collectExpired() {
        cacheService.collectExpired();
    }

    public void collectLFU() {
        cacheService.collectLFU();
    }

    public void collectAll() {
        cacheService.collectAll();
    }

    public void clear() {
        cacheService.clear();
    }

    public long size() {
        return cacheService.entries();
    }

    public long sizeInBytes() {
        return getMemoryManager().used();
    }

    public long capacityInBytes() {
        return getMemoryManager().capacity();
    }

    public void dump() {
        cacheService.dump();
    }

    public Serializer getSerializer() {
        return cacheService.getSerializer();
    }

    public MemoryManagerService<V> getMemoryManager() {
        return cacheService.getMemoryManager();
    }

    public Pointer<V> allocate(K key, Class<V> type, int size) {
        return cacheService.allocate(key, type, size);
    }

    public Set<K> getKeys() {
        return cacheService.getMap().keySet();
    }

    public boolean containsKey(K key) {
        return cacheService.getMap().containsKey(key);
    }

}
