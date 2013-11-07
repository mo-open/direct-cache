package net.dongliu.directcache.cache;

import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.memory.SlabsAllocator;
import net.dongliu.directcache.serialization.Serializer;
import net.dongliu.directcache.utils.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;

public final class BinaryCacheBuilder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The offheap memory size used in bytes.
     * default is the half of max direct memory
     */
    private long maxSize = VM.maxDirectMemory() / 2;

    /**
     * the initial used for cache. default is 256 mb.
     * current the parameter is not used.
     */
    private int initialSize = Size.Mb(512);

    private int concurrency = 256;

    private CacheEventListener listener;

    public BinaryCacheBuilder() {
    }

    public BinaryCache newCacheService() {
        Allocator allocator = SlabsAllocator.getSlabsAllocator(this.maxSize);
        BinaryCache cache = new BinaryCache(allocator, this.listener, this.concurrency);
        return cache;
    }

    /**
     * The offheap memory size used in bytes.
     * default is the half of max direct memory
     * @param maxSize
     * @return
     */
    public BinaryCacheBuilder setMaxSize(long maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    /**
     * the initial used for cache. default is 256 mb.
     * current the parameter is not used.
     */
    public BinaryCacheBuilder setInitialSize(int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    /**
     * The custom serializer.
     * @param serializer
     * @return
     */
    public BinaryCacheBuilder setSerializer(Serializer serializer) {
        return this;
    }

    public void setListener(CacheEventListener listener) {
        this.listener = listener;
    }

    /**
     * the cache concurrency num.
     * @param concurrency
     */
    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }
}
