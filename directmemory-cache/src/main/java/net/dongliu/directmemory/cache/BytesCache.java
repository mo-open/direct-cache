package net.dongliu.directmemory.cache;

import net.dongliu.directmemory.measures.Ram;
import net.dongliu.directmemory.memory.MemoryManager;
import net.dongliu.directmemory.memory.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Default implemnts of cacheService
 *
 * @author dongliu
 */
public class BytesCache {

    private static final Logger logger = LoggerFactory.getLogger(BytesCache.class);

    private ConcurrentMap<Object, Pointer> map;

    private MemoryManager memoryManager;

    private final Timer timer = new Timer(true);

    /**
     * Constructor
     */
    public BytesCache(ConcurrentMap<Object, Pointer> map, MemoryManager memoryManager) {
        this.map = map;
        this.memoryManager = memoryManager;
    }

    public void scheduleDisposalEvery(long period, TimeUnit unit) {
        scheduleDisposalEvery(unit.toMillis(period));
    }

    public void scheduleDisposalEvery(long period) {
        timer.schedule(new TimerTask() {
            public void run() {
                logger.info("begin scheduled disposal");

                collectExpired();
                collectLFU();

                logger.info("scheduled disposal complete");
            }
        }, period, period);

        logger.info("disposal scheduled every {} milliseconds", period);
    }

    public Pointer put(Object key, byte[] payload) {
        return store(key, payload, 0);
    }

    public Pointer put(Object key, byte[] payload, long expiresIn) {
        return store(key, payload, expiresIn);
    }

    private Pointer store(Object key, byte[] payload, long expiresIn) {
        // need synchronized?
        Pointer pointer = map.get(key);
        if (pointer != null) {
            memoryManager.free(pointer);
        }
        pointer = memoryManager.store(payload, expiresIn);
        if (pointer != null) {
            map.put(key, pointer);
        }
        return pointer;
    }

    public byte[] retrieve(Object key) {
        Pointer ptr = getPointer(key);
        if (ptr == null) {
            return null;
        }
        if (ptr.isExpired() || ptr.isFree()) {
            map.remove(key);
            if (!ptr.isFree()) {
                memoryManager.free(ptr);
            }
            return null;
        } else {
            return memoryManager.retrieve(ptr);
        }
    }

    public Pointer getPointer(Object key) {
        return map.get(key);
    }

    public void free(Object key) {
        Pointer p = map.remove(key);
        if (p != null) {
            memoryManager.free(p);
        }
    }

    public void free(Pointer pointer) {
        memoryManager.free(pointer);
    }

    public void collectExpired() {
        memoryManager.collectExpired();
        // still have to look for orphan (storing references to freed pointers) map entries
    }

    public void collectLFU() {
        memoryManager.collectLFU();
        // can possibly clear one whole buffer if it's too fragmented - investigate
    }

    public void collectAll() {
        Thread thread = new Thread() {
            public void run() {
                logger.info("begin disposal");
                collectExpired();
                collectLFU();
                logger.info("disposal complete");
            }
        };
        thread.start();
    }


    public void clear() {
        map.clear();
        memoryManager.clear();
        logger.info("Cache cleared");
    }

    public void close() throws IOException {
        memoryManager.close();
        logger.info("Cache closed");
    }

    public long entries() {
        return map.size();
    }

    public void dump(MemoryManager mms) {
        logger.info(format("off-heap - allocated: \t%1s", Ram.inMb(mms.capacity())));
        logger.info(format("off-heap - usedMemory:      \t%1s", Ram.inMb(mms.used())));
        logger.info(format("heap  - max: \t%1s", Ram.inMb(Runtime.getRuntime().maxMemory())));
        logger.info(format("heap     - allocated: \t%1s", Ram.inMb(Runtime.getRuntime().totalMemory())));
        logger.info(format("heap     - free : \t%1s", Ram.inMb(Runtime.getRuntime().freeMemory())));
        logger.info("************************************************");
    }

    public void dump() {
        if (!logger.isInfoEnabled()) {
            return;
        }

        logger.info("*** BytesCacheBuilder statistics ********************");

        dump(memoryManager);
    }

    public ConcurrentMap<Object, Pointer> getMap() {
        return map;
    }

    public void setMap(ConcurrentMap<Object, Pointer> map) {
        this.map = map;
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public void setMemoryManager(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }
}
