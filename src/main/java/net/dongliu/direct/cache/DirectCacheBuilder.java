package net.dongliu.direct.cache;

import net.dongliu.direct.utils.Size;

/**
 * @author dongliu
 */
public class DirectCacheBuilder {

    /**
     * Cache concurrent map concurrent level
     */
    private int concurrency = 256;

    /**
     * Cache concurrent map initial size of one segment.
     */
    private int initialSize = 1024;

    /**
     * cache map load factor
     */
    private float loadFactor = 0.75f;

    /**
     * min chunk size for slab allocator
     */
    private int chunkSize = 48;

    /**
     * chunk expand factor.
     */
    private float expandFactor = 1.25f;

    private long maxMemorySize = Size.Gb(1);

    private int slabSize = Size.Mb(4);

    private CacheEventListener cacheEventListener;

    protected DirectCacheBuilder() {
    }

    public DirectCacheBuilder concurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    public DirectCacheBuilder initialSize(int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public DirectCacheBuilder loadFactor(float loadFactor) {
        this.loadFactor = loadFactor;
        return this;
    }

    public DirectCacheBuilder minEntrySize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public DirectCacheBuilder expandFactor(float expandFactor) {
        this.expandFactor = expandFactor;
        return this;
    }

    public DirectCacheBuilder maxMemorySize(long maxMemorySize) {
        this.maxMemorySize = maxMemorySize;
        return this;
    }

    public DirectCacheBuilder cacheEventListener(CacheEventListener cacheEventListener) {
        this.cacheEventListener = cacheEventListener;
        return this;
    }

    public DirectCache build() {
        return new DirectCache(maxMemorySize, expandFactor, chunkSize, slabSize,
                initialSize, loadFactor, concurrency,
                cacheEventListener);
    }
}
