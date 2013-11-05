package net.dongliu.directcache.cache;

import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.struct.MemoryBuffer;
import net.dongliu.directcache.struct.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A cache store binary values.The base for all caches.
 *
 * @author dongliu
 */
public class BinaryCache {

    private static final Logger logger = LoggerFactory.getLogger(BinaryCache.class);

    private SelectableConcurrentHashMap map;

    private final Allocator allocator;

    /**
     * Constructor
     */
    public BinaryCache(Allocator allocator) {
        //TODO: add cache builder to set parameters.
        this.allocator = allocator;
        this.map = new SelectableConcurrentHashMap(allocator, 1000, 0.75f, 256, 0, null);
    }

    public void set(Object key, byte[] payload) {
        set(key, payload, 0);
    }

    /**
     * Put an element in the store if no element is currently mapped to the elements key.
     * @return the Pointer previously cached for this key, or null if none.
     */
    public byte[] putIfAbsent(Object key, byte[] payload) {
        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            Pointer oldPointer = null;
            Pointer pointer = store(key, payload);
            if (pointer != null) {
                oldPointer = map.putIfAbsent(key, pointer, pointer.getMemoryBuffer().getSize());
                //TODO: we need read value, but oldPointer is already freed.
                //byte[] oldValue = oldPointer.readValue();
                return null;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * whether the key exists.
     *
     * @return true if key exists.if key is expired still return true.
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
     * store the value, return pointer.
     *
     * @return the old pointer, null if no old value.
     */
    public void set(Object key, byte[] payload, int expiresIn) {
        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            Pointer oldPointer = null;
            Pointer pointer = store(key, payload);
            if (pointer != null) {
                if (expiresIn != 0) {
                    pointer.setTimeToIdle(expiresIn);
                }
                oldPointer = map.put(key, pointer, pointer.getMemoryBuffer().getSize());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * retrive value by key from cache.
     */
    public byte[] get(Object key) {
        Pointer pointer = retrievePointer(key);
        if (pointer == null) {
            return null;
        }
        return pointer.readValue();
    }

    /**
     * retrive value by key from cache.
     */
    private Pointer retrievePointer(Object key) {
        Pointer pointer = map.get(key);
        if (pointer == null) {
            return null;
        }

        if (!pointer.isExpired() && pointer.getLive().get()) {
            return pointer;
        }

        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            pointer = map.get(key);
            if (pointer.isExpired() || !pointer.getLive().get()) {
                map.remove(key);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    public void clear() {
        map.clear();
        logger.info("Cache cleared");
    }

    public void dispose() {
        map.clear();
        this.allocator.dispose();
        logger.info("Cache closed");
    }

    public long entries() {
        return map.size();
    }

    public void remove(Object key) {
        this.map.remove(key);
    }

    /**
     * allocate memory and store the payload, return the pointer.
     * @return the point.null if failed.
     */
    private Pointer store(Object key, byte[] payload) {
        MemoryBuffer buffer = allocator.allocate(payload.length);
        if (buffer == null) {
            return null;
        }

        Pointer p = new Pointer(buffer);
        buffer.write(payload);
        p.setKey(key);
        return p;
    }

    private Lock getWriteLock(Object key) {
        return map.lockFor(key).writeLock();
    }

    public long used() {
        return this.allocator.used();
    }

    public ReentrantReadWriteLock lockFor(Object key) {
        return map.lockFor(key);
    }
}
