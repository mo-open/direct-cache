package net.sf.ehcache.store;

import net.dongliu.directmemory.cache.BinaryCache;
import net.dongliu.directmemory.cache.BinaryCacheBuilder;
import net.dongliu.directmemory.serialization.SerializerFactory;
import net.sf.ehcache.*;
import net.sf.ehcache.concurrent.CacheLockProvider;
import net.sf.ehcache.concurrent.ReadWriteLockSync;
import net.sf.ehcache.concurrent.Sync;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.store.disk.StoreUpdateException;
import net.sf.ehcache.util.ratestatistics.AtomicRateStatistic;
import net.sf.ehcache.util.ratestatistics.RateStatistic;
import net.sf.ehcache.writer.CacheWriterManager;
import net.dongliu.directmemory.memory.struct.Pointer;
import net.dongliu.directmemory.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DirectMemoryStore extends AbstractStore implements TierableStore, PoolableStore {

    private static Logger logger = LoggerFactory.getLogger(BinaryCache.class);

    private final RateStatistic hitRate = new AtomicRateStatistic(1000, TimeUnit.MILLISECONDS);
    private final RateStatistic missRate = new AtomicRateStatistic(1000, TimeUnit.MILLISECONDS);
    private volatile Status status;

    protected BinaryCache binaryCache;

    private final Ehcache cache;

    private Serializer serializer;

    public static DirectMemoryStore create(Ehcache cache, Pool<PoolableStore> offHeapPool) {
        return new DirectMemoryStore(cache, offHeapPool, false);
    }

    protected DirectMemoryStore(Ehcache cache, Pool<PoolableStore> offHeapPool,
                                boolean doNotifications) {
        //TODO: implemets and use offHeapPool
        this.status = Status.STATUS_UNINITIALISED;
        // we have checked the offHeapBytes setting before creatint DirectMemoryStore.
        // asume it's all right here.
        long offHeapSizeBytes = cache.getCacheConfiguration().getMaxMemoryOffHeapInBytes();
        this.cache = cache;

        binaryCache = createCacheService(offHeapSizeBytes);

        serializer = SerializerFactory.createNewSerializer();
        this.status = Status.STATUS_ALIVE;
    }

    private BinaryCache createCacheService(long size) {
        return new BinaryCacheBuilder()
                .setSize(size)
                .setInitialCapacity(BinaryCacheBuilder.DEFAULT_INITIAL_CAPACITY)
                .newCacheService();
    }

    @Override
    public void unpinAll() {
        //to be implemeted.
    }

    @Override
    public boolean isPinned(Object key) {
        //to be implemeted.
        return false;
    }

    @Override
    public void setPinned(Object key, boolean pinned) {
        //TODO: pinned to be implemeted.
    }

    @Override
    public boolean put(Element element) throws CacheException {
        if (element == null) {
            return true;
        }
        boolean exists = binaryCache.getMap().containsKey(element.getKey());
        Pointer pointer = binaryCache.put(element.getObjectKey(), ElementToBytes(element));

        if (pointer == null) {
            throw new CacheException("Put element failed.");
        }

        return !exists;
    }

    @Override
    public boolean putWithWriter(Element element, CacheWriterManager writerManager)
            throws CacheException {
        //need synchronized?
        boolean newPut = put(element);
        if (writerManager != null) {
            try {
                writerManager.put(element);
            } catch (RuntimeException e) {
                throw new StoreUpdateException(e, !newPut);
            }
        }
        return newPut;
    }

    @Override
    public Element get(Object key) {
        if (key == null) {
            return null;
        }

        byte[] bytes = binaryCache.retrieve(key);
        if (bytes == null) {
            missRate.event();
        } else {
            hitRate.event();
        }
        return BytesToElement(bytes);
    }

    @Override
    public Element getQuiet(Object key) {
        byte[] bytes = binaryCache.retrieve(key);
        if (bytes == null) {
            return null;
        }
        return BytesToElement(bytes);
    }

    @Override
    public List<Object> getKeys() {
        return new ArrayList<Object>(binaryCache.getMap().keySet());
    }

    @Override
    public Element remove(Object key) {
        if (key == null) {
            return null;
        }
        Element element = getQuiet(key);
        binaryCache.free(key);
        return element;
    }

    @Override
    public Element removeWithWriter(Object key, CacheWriterManager writerManager) throws CacheException {
        //need synchronized?
        if (key == null) {
            return null;
        }
        Element removed = remove(key);
        if (writerManager != null) {
            writerManager.remove(new CacheEntry(key, removed));
        }
        return removed;
    }

    @Override
    public void removeAll() throws CacheException {
        binaryCache.clear();
    }

    @Override
    public Element putIfAbsent(Element element) throws NullPointerException {
        //need synchronized?
        Element returnElement = get(element.getObjectKey());
        if (returnElement == null) {
            put(element);
        }
        return returnElement;
    }

    @Override
    public Element removeElement(Element element, ElementValueComparator comparator)
            throws NullPointerException {
        if (element == null || element.getObjectKey() == null) {
            return null;
        }
        Pointer pointer = binaryCache.getPointer(element.getObjectKey());
        if (pointer == null) {
            return null;
        }

        Element toRemove = getQuiet(element.getObjectKey());
        if (comparator.equals(element, toRemove)) {
            binaryCache.free(element.getObjectKey());
            return toRemove;
        } else {
            return null;
        }
    }

    @Override
    public boolean replace(Element old, Element element, ElementValueComparator comparator)
            throws NullPointerException, IllegalArgumentException {
        if (element == null || element.getObjectKey() == null) {
            return false;
        }
        Pointer pointer = binaryCache.getPointer(element.getObjectKey());
        if (pointer == null) {
            return false;
        }

        Element toUpdate = getQuiet(element.getObjectKey());
        if (comparator.equals(old, toUpdate)) {
            binaryCache.put(element.getObjectKey(), ElementToBytes(element));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Element replace(Element element) throws NullPointerException {
        Pointer pointer = binaryCache.getPointer(element.getObjectKey());
        if (pointer == null) {
            return null;
        }

        Element toUpdate = getQuiet(element.getObjectKey());
        if (toUpdate != null) {
            binaryCache.put(element.getObjectKey(), ElementToBytes(element));
            return toUpdate;
        } else {
            return null;
        }
    }

    @Override
    public synchronized void dispose() {
        if (status.equals(Status.STATUS_SHUTDOWN)) {
            return;
        }
        status = Status.STATUS_SHUTDOWN;
        flush();
    }

    @Override
    public int getSize() {
        return getOffHeapSize();
    }

    // inMemory means on heap here and below.
    @Override
    public int getInMemorySize() {
        return 0;
    }

    @Override
    public int getOffHeapSize() {
        long size = binaryCache.entries();
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) size;
        }
    }

    @Override
    public int getOnDiskSize() {
        return 0;
    }

    @Override
    public int getTerracottaClusteredSize() {
        return 0;
    }

    @Override
    public long getInMemorySizeInBytes() {
        return 0;
    }

    @Override
    public long getOffHeapSizeInBytes() {
        return binaryCache.getMemoryManager().used();
    }

    @Override
    public long getOnDiskSizeInBytes() {
        return 0;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public boolean containsKey(Object key) {
        return containsKeyOffHeap(key);
    }

    @Override
    public boolean containsKeyOnDisk(Object key) {
        return false;
    }

    @Override
    public boolean containsKeyOffHeap(Object key) {
        return binaryCache.getMap().containsKey(key);
    }

    @Override
    public boolean containsKeyInMemory(Object key) {
        return false;
    }

    @Override
    public void expireElements() {
        //to be implemented.
    }

    @Override
    public void flush() {
        binaryCache.clear();
    }

    @Override
    public boolean bufferFull() {
        //never backs up/ no buffer usedMemory.
        return false;
    }

    @Override
    public Policy getInMemoryEvictionPolicy() {
        return null;
    }

    @Override
    public void setInMemoryEvictionPolicy(Policy policy) {
        //
    }

    @Override
    public Object getInternalContext() {
        // This should not be usedMemory, and will generally return null
        return null;
    }

    @Override
    public Object getMBean() {
        return null;
    }

// -------------------------- implements  PoolableStore ---------------------------------------
// seems that  PoolableStore not suitable for offheap.

    @Override
    public boolean evictFromOnHeap(int count, long size) {
        return false;
    }

    @Override
    public boolean evictFromOnDisk(int count, long size) {
        return false;
    }

    @Override
    public float getApproximateDiskHitRate() {
        return 0;
    }

    @Override
    public float getApproximateDiskMissRate() {
        return 0;
    }

    @Override
    public long getApproximateDiskCountSize() {
        return 0;
    }

    @Override
    public long getApproximateDiskByteSize() {
        return 0;
    }

    @Override
    public float getApproximateHeapHitRate() {
        return 0;
    }

    @Override
    public float getApproximateHeapMissRate() {
        return 0;
    }

    @Override
    public long getApproximateHeapCountSize() {
        return 0;
    }

    @Override
    public long getApproximateHeapByteSize() {
        return 0;
    }

// -------------------------- implements TierableStore ---------------------------------------

    @Override
    public void fill(Element element) {
        if (element == null) {
            return;
        }
        binaryCache.put(element.getObjectKey(), ElementToBytes(element));
    }

    @Override
    public boolean removeIfNotPinned(Object key) {
        remove(key);
        return true;
    }

    @Override
    public boolean isTierPinned() {
        return false;
    }

    @Override
    public Set getPresentPinnedKeys() {
        return Collections.emptySet();
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public void removeNoReturn(Object key) {
        remove(key);
    }

    /**
     * LockProvider implementation that uses the segment locks.
     */
    private class LockProvider implements CacheLockProvider {

        @Override
        public Sync getSyncForKey(Object key) {
            Pointer pointer = binaryCache.getPointer(key);
            return new ReadWriteLockSync(new ReentrantReadWriteLock());
        }
    }

    private byte[] ElementToBytes(Element element) {
        try {
            return serializer.serialize(element);
        } catch (IOException e) {
            // should not happen.
            throw new RuntimeException(e);
        }
    }

    private Element BytesToElement(byte[] bytes) {
        try {
            return serializer.deserialize(bytes, Element.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
