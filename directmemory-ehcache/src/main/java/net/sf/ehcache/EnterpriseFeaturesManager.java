package net.sf.ehcache;

import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.store.*;
import net.sf.ehcache.transaction.SoftLockFactory;
import net.sf.ehcache.transaction.SoftLockManager;
import net.sf.ehcache.transaction.TransactionIDFactory;
import net.sf.ehcache.writer.writebehind.WriteBehind;

/**
 * Provide enterprise features(now only directmomery) for ehcache.
 * The class name is hard coded in ehcache, so we must use the same package and class name.
 *
 * @author dongliu
 */
public class EnterpriseFeaturesManager
        implements FeaturesManager {

    private CacheManager cacheManager;

    public EnterpriseFeaturesManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public WriteBehind createWriteBehind(Cache cache) {
        return null;
    }

    @Override
    public Store createStore(Cache cache, Pool<PoolableStore> onHeapPool, Pool<PoolableStore> onDiskPool) {

        if(cache.getCacheConfiguration() == null || !cache.getCacheConfiguration().isOverflowToOffHeap()
                || cache.getCacheConfiguration().getMaxBytesLocalOffHeap() <= 0) {
            return createNonOffHeapStore(cache, onHeapPool, onDiskPool);
        }

        //TODO: combine with offHeapPool & onDiskPool
        return OffHeapBackendStore.create(cache, onHeapPool, null);
    }

    /**
     * if overFlowToOffHeap is disabled, or maxBytesLocalOffHeap is less than or equal zero,
     * create a non-offheap store.
     * @param cache
     * @param onHeapPool
     * @param onDiskPool
     * @return
     */
    @SuppressWarnings("deprecation")
    private Store createNonOffHeapStore (Cache cache, Pool<PoolableStore> onHeapPool, Pool<PoolableStore> onDiskPool){
        Store store;
        CacheConfiguration configuration = cache.getCacheConfiguration();
        PersistenceConfiguration persistence = configuration.getPersistenceConfiguration();
        if (persistence != null && PersistenceConfiguration.Strategy.LOCALRESTARTABLE.equals(persistence.getStrategy())) {
            throw new CacheException("Cache " + configuration.getName()
                    + " cannot be configured because the enterprise features manager could not be found. "
                    + "You must use an enterprise version of Ehcache to successfully enable enterprise persistence.");
        }

        // Warning: We skip useClassicLru setting.
        // we cannot get useClassicLru from cache because it's private.
        if (configuration.isOverflowToDisk()) {
            store = DiskBackedMemoryStore.create(cache, onHeapPool, onDiskPool);
        } else {
            store = MemoryOnlyStore.create(cache, onHeapPool);
        }
        return store;
    }

    @Override
    public TransactionIDFactory createTransactionIDFactory() {
        return null;
    }

    @Override
    public SoftLockManager createSoftLockManager(Ehcache cache, SoftLockFactory lockFactory) {
        return null;
    }

    @Override
    public void startup() {

    }

    @Override
    public void dispose() {

    }

}
