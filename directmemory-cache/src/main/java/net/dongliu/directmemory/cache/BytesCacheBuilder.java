package net.dongliu.directmemory.cache;

import com.google.common.collect.MapMaker;
import net.dongliu.directmemory.memory.MemoryManager;
import net.dongliu.directmemory.memory.MemoryManagerImpl;
import net.dongliu.directmemory.memory.struct.Pointer;
import net.dongliu.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static net.dongliu.directmemory.measures.In.seconds;
import static net.dongliu.directmemory.serialization.SerializerFactory.createNewSerializer;

public final class BytesCacheBuilder {

    public static final int DEFAULT_CONCURRENCY_LEVEL = 4;

    public static final int DEFAULT_INITIAL_CAPACITY = 100000;

    public static final int DEFAULT_DISPOSAL_TIME = 10; // seconds

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private int size;

    private int initialCapacity = DEFAULT_INITIAL_CAPACITY;

    private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;

    private long disposalTime = seconds(DEFAULT_DISPOSAL_TIME);

    private ConcurrentMap<Object, Pointer> map;


    private MemoryManager memoryManager;

    public BytesCacheBuilder() {
        // does nothing
    }

    public BytesCacheBuilder(BytesCacheBuilder prototype) {
        checkArgument(prototype != null, "Impossible to create a BytesCacheBuilder instance from a null prototype");

        size = prototype.size;
        initialCapacity = prototype.initialCapacity;
        concurrencyLevel = prototype.concurrencyLevel;
        disposalTime = prototype.disposalTime;

        map = prototype.map;
        memoryManager = prototype.memoryManager;
    }

    public BytesCacheBuilder setNumberOfBuffers(int numberOfBuffers) {
        checkArgument(numberOfBuffers > 0, "Impossible to create a CacheService with a number of buffers lesser than 1");
        return this;
    }

    public BytesCacheBuilder setSize(int size) {
        checkArgument(size > 0, "Impossible to create a CacheService with a size lesser than 1");
        this.size = size;
        return this;
    }

    public BytesCacheBuilder setInitialCapacity(int initialCapacity) {
        checkArgument(initialCapacity > 0, "Impossible to create a CacheService with an initialCapacity lesser than 1");
        this.initialCapacity = initialCapacity;
        return this;
    }

    public BytesCacheBuilder setConcurrencyLevel(int concurrencyLevel) {
        checkArgument(concurrencyLevel > 0, "Impossible to create a CacheService with a concurrencyLevel lesser than 1");
        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    public BytesCacheBuilder setDisposalTime(long disposalTime) {
        checkArgument(disposalTime > 0, "Impossible to create a CacheService with a disposalTime lesser than 1");
        this.disposalTime = disposalTime;
        return this;
    }

    public BytesCacheBuilder setMap(ConcurrentMap<Object, Pointer> map) {
        checkArgument(map != null, "Impossible to create a CacheService with a null map");
        this.map = map;
        return this;
    }

    public BytesCacheBuilder setSerializer(Serializer serializer) {
        checkArgument(serializer != null, "Impossible to create a CacheService with a null serializer");
        return this;
    }

    public BytesCacheBuilder setMemoryManager(MemoryManager memoryManager) {
        checkArgument(memoryManager != null, "Impossible to create a CacheService with a null memoryManager");
        this.memoryManager = memoryManager;
        return this;
    }

    public BytesCache newCacheService() {
        if (map == null) {
            map = new MapMaker().concurrencyLevel(concurrencyLevel).initialCapacity(initialCapacity).makeMap();
        }
        if (memoryManager == null) {
            memoryManager = new MemoryManagerImpl();
        }

        logger.info("initializing");
        logger.info(format("initial capacity: \t%1d", initialCapacity));
        logger.info(format("concurrency level: \t%1d", concurrencyLevel));

        memoryManager.init(size);

        logger.info("initialized");


        BytesCache cacheService = new BytesCache(map, memoryManager);
        cacheService.scheduleDisposalEvery(disposalTime);
        return cacheService;
    }

}
