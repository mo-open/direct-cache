package net.sf.ehcache.store.offheap;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.pool.impl.UnboundedPool;
import net.sf.ehcache.search.Results;
import net.sf.ehcache.search.attribute.AttributeExtractor;
import net.sf.ehcache.search.impl.SearchManager;
import net.sf.ehcache.store.FrontEndCacheTier;
import net.sf.ehcache.store.MemoryStore;
import net.sf.ehcache.store.Store;
import net.sf.ehcache.store.StoreQuery;
import net.sf.ehcache.store.disk.DiskStore;
import org.apache.directmemory.ehcache.DirectMemoryStore;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * This class is simply a connector class into the EHCache for OffHeap.
 *
 * @author michaelandrepearce
 */
public class OffHeapStore {

    private static final WeakHashMap<CacheManager, Pool<PoolableStore>> OFFHEAP_POOLS =
            new WeakHashMap<CacheManager, Pool<PoolableStore>>();

    public static Store create(Ehcache cache, String diskStorePath, Pool<PoolableStore> onHeapPool,
                               Pool<PoolableStore> onDiskPool) {

        CacheConfiguration config = cache.getCacheConfiguration();
        MemoryStore memoryStore = createMemoryStore(cache, onHeapPool);
        DirectMemoryStore offHeapStore = createOffHeapStore(cache);
        DiskStore diskStore = null; //need to implement disk backing to store.
        Store store = null;
        if (diskStore == null) {
            store = new FrontEndCacheTier<MemoryStore, DirectMemoryStore>(memoryStore, offHeapStore,
                    config.getCopyStrategy(),
                    new MockSearchManager(),
                    config.isCopyOnWrite(),
                    config.isCopyOnRead()) {

                @Override
                public Object getMBean() {
                    return this.authority.getMBean();
                }

            };
        }
        return store;
    }

    /**
     * TODO create a real {@link SearchManager}
     */
    private static class MockSearchManager implements SearchManager {

        @Override
        public Results executeQuery(String cacheName, StoreQuery query,
                                    Map<String, AttributeExtractor> attributeExtractors) {
            return null;
        }

        @Override
        public void put(String cacheName, int segmentId, String uniqueKey,
                        byte[] serializedKey, Element element, Map<String, AttributeExtractor> extractors) {

        }

        @Override
        public void remove(String cacheName, String uniqueKey, int segmentId) {

        }

        @Override
        public void clear(String cacheName, int segmentId) {

        }

        @Override
        public void move(String cacheName, int segmentId, String existingKey, String newKey) {

        }
    }

    /**
     * Creates a persitent-to-disk store for the given cache, using the given
     * disk path. Heap and disk usage are not tracked by the returned store.
     *
     * @param cache         cache that fronts this store
     * @param diskStorePath disk path to store data in
     * @return a fully initialized store
     */
    public static Store create(Ehcache cache, String diskStorePath) {
        return create(cache, diskStorePath, new UnboundedPool(), new UnboundedPool());
    }

    private static MemoryStore createMemoryStore(Ehcache cache, Pool<PoolableStore> onHeapPool) {
        return MemoryStore.create(cache, onHeapPool);
    }

    /**
     * <b>do not use directly as can change</b>
     *
     * @param cache
     * @return
     */
    public static DirectMemoryStore createOffHeapStore(Ehcache cache) {
        Pool<PoolableStore> offHeapPool = null;
        if (cache.getCacheConfiguration().getMaxBytesLocalOffHeap() == 0L) {
            offHeapPool = getOffHeapPool(cache.getCacheManager());
        }
        return new DirectMemoryStore(cache, offHeapPool);
    }

    /**
     * <b>do not use directly as can change</b>
     *
     * @param manager
     * @return
     */
    public static Pool<PoolableStore> getOffHeapPool(CacheManager manager) {
        Pool<PoolableStore> pool;
        synchronized (OFFHEAP_POOLS) {
            pool = OFFHEAP_POOLS.get(manager);
            if (pool == null) {
                pool = new UnboundedPool();
                OFFHEAP_POOLS.put(manager, pool);
            }
        }
        return pool;
    }

}
