package org.apache.directmemory.cache;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;

import java.io.IOException;

/**
 * The cache api.
 */
public class Cache {

    private static final DirectMemory<String, Object> builder = new DirectMemory<String, Object>();

    private static CacheService<String, Object> cacheService = builder.newCacheService();

    private Cache() {
        // not instantiable
    }

    public static void scheduleDisposalEvery(long l) {
        // store to builder
        builder.setDisposalTime(l);

        cacheService.scheduleDisposalEvery(l);
    }

    public static void init(int numberOfBuffers, int size, int initialCapacity, int concurrencyLevel) {
        cacheService = builder.setNumberOfBuffers(numberOfBuffers)
                        .setInitialCapacity(initialCapacity)
                        .setConcurrencyLevel(concurrencyLevel)
                        .setSize(size)
                        .newCacheService();
    }

    public static void init(int numberOfBuffers, int size) {
        init(numberOfBuffers, size, DirectMemory.DEFAULT_INITIAL_CAPACITY, DirectMemory.DEFAULT_CONCURRENCY_LEVEL);
    }

    public static Pointer<Object> putByteArray(String key, byte[] payload, int expiresIn) {
        return cacheService.putByteArray(key, payload, expiresIn);
    }

    public static Pointer<Object> putByteArray(String key, byte[] payload) {
        return cacheService.putByteArray(key, payload);
    }

    public static Pointer<Object> put(String key, Object object) {
        return cacheService.put(key, object);
    }

    public static Pointer<Object> put(String key, Object object, int expiresIn) {
        return cacheService.put(key, object, expiresIn);
    }

    public static byte[] retrieveByteArray(String key) {
        return cacheService.retrieveByteArray(key);
    }

    public static Object retrieve(String key) {
        return cacheService.retrieve(key);
    }

    public static Pointer<Object> getPointer(String key) {
        return cacheService.getPointer(key);
    }

    public static void free(String key) {
        cacheService.free(key);
    }

    public static void free(Pointer<Object> pointer) {
        cacheService.free(pointer);
    }

    public static void collectExpired() {
        cacheService.collectExpired();
    }

    public static void collectLFU() {
        cacheService.collectLFU();
    }

    public static void collectAll() {
        cacheService.collectAll();
    }

    public static void clear() {
        cacheService.clear();
    }

    public static void close()
            throws IOException {
        cacheService.close();
    }

    public static long entries() {
        return cacheService.entries();
    }

    public static void dump() {
        cacheService.dump();
    }

    public static Serializer getSerializer() {
        return cacheService.getSerializer();
    }

    public static MemoryManager<Object> getMemoryManager() {
        return cacheService.getMemoryManager();
    }

    public static Pointer<Object> allocate(String key, int size) {
        return cacheService.allocate(key, Object.class, size);
    }

}
