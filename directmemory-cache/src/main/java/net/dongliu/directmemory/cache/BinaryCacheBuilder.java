package net.dongliu.directmemory.cache;

import net.dongliu.directmemory.memory.MemoryManager;
import net.dongliu.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static net.dongliu.directmemory.measures.In.seconds;

public final class BinaryCacheBuilder {

    public static final int DEFAULT_INITIAL_CAPACITY = 100000;

    public static final int DEFAULT_DISPOSAL_TIME = 10; // seconds

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private long size;

    private int initialCapacity = DEFAULT_INITIAL_CAPACITY;

    private long disposalTime = seconds(DEFAULT_DISPOSAL_TIME);

    private MemoryManager memoryManager;

    public BinaryCacheBuilder() {
        // does nothing
    }

    public BinaryCacheBuilder setSize(long size) {
        this.size = size;
        return this;
    }

    public BinaryCacheBuilder setInitialCapacity(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        return this;
    }

    public BinaryCacheBuilder setDisposalTime(long disposalTime) {
        this.disposalTime = disposalTime;
        return this;
    }

    public BinaryCacheBuilder setSerializer(Serializer serializer) {
        return this;
    }

    public BinaryCacheBuilder setMemoryManager(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
        return this;
    }

    public BinaryCache newCacheService() {
        if (memoryManager == null) {
            memoryManager = new MemoryManager(size);
        }

        BinaryCache cacheService = new BinaryCache(memoryManager);
        cacheService.scheduleDisposalEvery(disposalTime);
        return cacheService;
    }

}
