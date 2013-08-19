package net.dongliu.directmemory.cache;

import net.dongliu.directmemory.memory.Allocator;
import net.dongliu.directmemory.memory.SlabsAllocator;
import net.dongliu.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BinaryCacheBuilder {

    public static final int DEFAULT_INITIAL_CAPACITY = 100000;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private long size;

    private int initialCapacity = DEFAULT_INITIAL_CAPACITY;

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

    public BinaryCacheBuilder setSerializer(Serializer serializer) {
        return this;
    }

    public BinaryCache newCacheService() {
        Allocator allocator = SlabsAllocator.getSlabsAllocator(this.size);
        BinaryCache cacheService = new BinaryCache(allocator);
        return cacheService;
    }

}
