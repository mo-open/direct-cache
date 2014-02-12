package net.sf.ehcache.store;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.search.impl.SearchManager;

/**
 * A direct-memory-only store with support for all caching features.
 *
 * @author Dong Liu
 */
public class DirectMemoryOnlyStore extends FrontEndCacheTier<NullStore, DirectMemoryStore> {

    /**
     * Create a MemoryOnlyStore
     *
     * @param cacheConfiguration the cache configuration
     * @param authority          the memory store
     */
    protected DirectMemoryOnlyStore(CacheConfiguration cacheConfiguration, DirectMemoryStore authority,
                                    SearchManager searchManager) {
        super(NullStore.create(), authority, cacheConfiguration.getCopyStrategy(), searchManager,
                cacheConfiguration.isCopyOnWrite(), cacheConfiguration.isCopyOnRead());
    }

    /**
     * Create an instance of MemoryOnlyStore
     *
     * @param cache the cache
     * @return an instance of MemoryOnlyStore
     */
    public static Store create(Ehcache cache) {
        final DirectMemoryStore memoryStore = NotifyingDirectMemoryStore.create(cache);
        return new DirectMemoryOnlyStore(cache.getCacheConfiguration(), memoryStore, null);
    }

    @Override
    public void setInMemoryEvictionPolicy(final Policy policy) {
        authority.setInMemoryEvictionPolicy(policy);
    }

    @Override
    public Policy getInMemoryEvictionPolicy() {
        return authority.getInMemoryEvictionPolicy();
    }

    public Object getMBean() {
        return null;
    }

}
