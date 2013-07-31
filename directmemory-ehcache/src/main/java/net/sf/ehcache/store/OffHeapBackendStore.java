package net.sf.ehcache.store;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.store.DirectMemoryStore;

import java.io.Serializable;

/**
 * combine memory store and offheapstore.
 * @author dongliu
 */
public class OffHeapBackendStore  extends FrontEndCacheTier<MemoryStore, DirectMemoryStore> {

    /**
     * Constructor for FrontEndCacheTier
     *
     * @param memoryStore         the caching tier
     */
    private OffHeapBackendStore(CacheConfiguration cacheConfiguration, MemoryStore memoryStore,
                                DirectMemoryStore directMemoryStore) {
        //TODO: implements a searchManager.
        super(memoryStore, directMemoryStore, cacheConfiguration.getCopyStrategy(), null,
                cacheConfiguration.isCopyOnWrite(), cacheConfiguration.isCopyOnRead());
    }

    /**
     * Create a DiskBackedMemoryStore instance
     * @param cache the cache
     * @param onHeapPool the pool tracking on-heap usage
     * @param offHeapPool the pool tracking on-disk usage
     * @return a DiskBackedMemoryStore instance
     */
    public static Store create(Ehcache cache, Pool onHeapPool, Pool offHeapPool) {
        final MemoryStore memoryStore = createMemoryStore(cache, onHeapPool);
        DirectMemoryStore store = DirectMemoryStore.create(cache.getCacheConfiguration(), offHeapPool);
        return new OffHeapBackendStore(cache.getCacheConfiguration(), memoryStore, store);
    }

    private static MemoryStore createMemoryStore(Ehcache cache, Pool onHeapPool) {
        return MemoryStore.create(cache, onHeapPool);
    }

    /**
     * {@inheritDoc}
     */
    public Object getMBean() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean notifyEvictionFromCache(final Serializable key) {
        return super.notifyEvictionFromCache(key);
    }
}
