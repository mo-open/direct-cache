package net.sf.ehcache.store;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.store.DirectMemoryStore;

import java.io.Serializable;

/**
 * combine memory store and direct memory store.
 * @author dongliu
 */
public class OffHeapBackendStore extends FrontEndCacheTier<MemoryStore, DirectMemoryStore> {

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
     * @return a DiskBackedMemoryStore instance
     */
    public static Store create(Ehcache cache, Pool onHeapPool) {
        final MemoryStore memoryStore = MemoryStore.create(cache, onHeapPool);
        DirectMemoryStore store = DirectMemoryStore.create(cache);
        return new OffHeapBackendStore(cache.getCacheConfiguration(), memoryStore, store);
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
