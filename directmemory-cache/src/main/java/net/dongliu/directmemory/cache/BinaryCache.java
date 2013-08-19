package net.dongliu.directmemory.cache;

import net.dongliu.directmemory.memory.Allocator;
import net.dongliu.directmemory.struct.MemoryBuffer;
import net.dongliu.directmemory.struct.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * A cache store binary values.The base for all caches.
 *
 * @author dongliu
 */
public class BinaryCache implements Cloneable {

    private static final Logger logger = LoggerFactory.getLogger(BinaryCache.class);

    private SelectableConcurrentHashMap map;

    private final Allocator allocator;

    /**
     * Constructor
     */
    public BinaryCache(Allocator allocator) {
        //TODO: add cache builder to set parameters.
        this.map = new SelectableConcurrentHashMap(false, 1000, 0.75f, 256, 0, null);
        this.allocator = allocator;
    }

    public Pointer put(Object key, byte[] payload) {
        return put(key, payload, 0);
    }

    /**
     * whether the key exists.
     *
     * @param key
     * @return true if key exists.if key is expired still return true.
     */
    public boolean exists(Object key) {
        return this.map.containsKey(key);
    }

    /**
     * return all keys cached.
     *
     * @return
     */
    public Collection<Object> keys() {
        return this.map.keySet();
    }

    /**
     * store the value, return pointer.
     *
     * @param key
     * @param payload
     * @param expiresIn
     * @return pointer to value, null if failed.
     */
    public Pointer put(Object key, byte[] payload, long expiresIn) {
        // need synchronized?
        Pointer pointer = map.get(key);
        if (pointer != null) {
//            if (pointer.getCapacity() > payload.length) {
//                pointer.getMemoryBuffer().write(payload);
//                return pointer;
//            }
        }
        pointer = store(key, payload);
        if (pointer != null) {
            if (expiresIn != 0) {
                pointer.setExpiration(System.currentTimeMillis() + expiresIn);
            } else {
                pointer.setExpiration(0);
            }
            map.put(key, pointer, pointer.getMemoryBuffer().getSize());
        }
        return pointer;
    }

    /**
     * retrive value by key from cache.
     *
     * @param key
     * @return
     */
    public byte[] retrieve(Object key) {
        Pointer pointer = map.get(key);
        if (pointer == null) {
            return null;
        }
        if (pointer.isExpired() || !pointer.getLive().get()) {
            //TODO: need sync
            map.remove(key);
            return null;
        } else {
            return pointer.getValue();
        }
    }

    public void clear() {
        for (Pointer pointer : map.values()) {
            pointer.free();
        }
        map.clear();
        logger.info("Cache cleared");
    }

    public void close() throws IOException {
        map.clear();
        this.allocator.close();
        logger.info("Cache closed");
    }

    public long entries() {
        return map.size();
    }

    public void remove(Object key) {
        this.map.remove(key);
    }

    private Pointer store(Object key, byte[] payload) {
        MemoryBuffer buffer = allocator.allocate(payload.length);
        if (buffer == null) {
            return null;
        }

        Pointer p = new Pointer(buffer, allocator);
        buffer.write(payload);
        p.setKey(key);
        return p;
    }

    public long used() {
        return this.allocator.used();
    }

    public Pointer getPointer(Object key) {
        return map.get(key);
    }
}
