package net.sf.ehcache.store;

import net.dongliu.directcache.cache.CacheHashMap;
import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.memory.SlabsAllocator;
import net.dongliu.directcache.serialization.SerializerFactory;
import net.dongliu.directcache.struct.MemoryBuffer;
import net.dongliu.directcache.struct.ValueWrapper;
import net.sf.ehcache.*;
import net.sf.ehcache.concurrent.CacheLockProvider;
import net.sf.ehcache.concurrent.ReadWriteLockSync;
import net.sf.ehcache.concurrent.Sync;
import net.sf.ehcache.pool.PoolableStore;
import net.sf.ehcache.store.disk.StoreUpdateException;
import net.sf.ehcache.util.ratestatistics.AtomicRateStatistic;
import net.sf.ehcache.util.ratestatistics.RateStatistic;
import net.sf.ehcache.writer.CacheWriterManager;
import net.dongliu.directcache.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * direct memory store impl.
 * @author dongliu
 */
public class DirectMemoryStore extends AbstractStore implements TierableStore, PoolableStore {

    private static Logger logger = LoggerFactory.getLogger(DirectMemoryStore.class);

    private final RateStatistic hitRate = new AtomicRateStatistic(1000, TimeUnit.MILLISECONDS);
    private final RateStatistic missRate = new AtomicRateStatistic(1000, TimeUnit.MILLISECONDS);
    private volatile Status status;

    private CacheHashMap map;

    private final Allocator allocator;

    private final Ehcache cache;

    private Serializer serializer;

    private volatile CacheLockProvider lockProvider;

    public static DirectMemoryStore create(Ehcache cache) {
        return new DirectMemoryStore(cache, false);
    }

    protected DirectMemoryStore(Ehcache cache, boolean doNotifications) {
        this.status = Status.STATUS_UNINITIALISED;
        // we have checked the offHeapBytes setting before creatint DirectMemoryStore.
        // asume it's all right here.
        long offHeapSizeBytes = cache.getCacheConfiguration().getMaxMemoryOffHeapInBytes();
        this.cache = cache;

        this.allocator = SlabsAllocator.getSlabsAllocator(offHeapSizeBytes);
        this.map = new CacheHashMap(allocator, 1000, 0.75f, 256, null);

        serializer = SerializerFactory.createNewSerializer();
        this.status = Status.STATUS_ALIVE;
    }

    @Override
    public void unpinAll() {
    }

    @Override
    public boolean isPinned(Object key) {
        return false;
    }

    @Override
    public void setPinned(Object key, boolean pinned) {
        throw new UnsupportedOperationException("Pinned element is not support.");
    }

    @Override
    public boolean put(Element element) throws CacheException {
        if (element == null) {
            return true;
        }

        Object key = element.getKey();
        if (key == null) {
            return true;
        }

        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            ValueWrapper oldValueWrapper = null;
            ValueWrapper valueWrapper = store(element);
            if (valueWrapper != null) {
                oldValueWrapper = map.put(key, valueWrapper);
            }
            return oldValueWrapper == null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * allocate memory and store the payload, return the pointer.
     * @return the point.null if failed.
     */
    private ValueWrapper store(Element element) {
        //TODO: if element.getValue() is null
        Object value = element.getValue();
        byte[] bytes = objectToBytes(value);
        MemoryBuffer buffer = allocator.allocate(bytes.length);
        if (buffer == null) {
            return null;
        }

        ValueWrapper p = new ValueWrapper(buffer);
        buffer.write(bytes);
        p.setKey(element.getKey());
        p.setExpiry(element.getTimeToIdle());
//        p.setTimeToLive(element.getTimeToLive());
//        p.setHitCount(element.getHitCount());
//        p.setVersion(element.getVersion());
        p.setLastUpdateTime(element.getLastUpdateTime());
        return p;
    }

    @Override
    public boolean putWithWriter(Element element, CacheWriterManager writerManager)
            throws CacheException {
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

        Element e = getQuiet(key);
        if (e == null) {
            missRate.event();
        } else {
            hitRate.event();
        }
        return e;
    }

    /**
     * Gets an Element from the Store, without updating statistics.
     * @param key
     * @return
     */
    @Override
    public Element getQuiet(Object key) {
        if (key == null) {
            return null;
        }

        ValueWrapper valueWrapper = map.get(key);
        if (valueWrapper == null) {
            return null;
        }
        if (valueWrapper.isExpired() || !valueWrapper.isLive()) {
            Lock lock = getWriteLock(key);
            lock.lock();
            try {
                valueWrapper = map.get(key);
                if (valueWrapper.isExpired() || !valueWrapper.isLive()) {
                    map.remove(key);
                }
            } finally {
                lock.unlock();
            }
            return null;
        }

        //TODO: add creation & lastAccessTime.
        long creation = valueWrapper.getLastUpdateTime();
        long lastAccessTime = valueWrapper.getLastUpdateTime();
        int hitCount = 1;
        Object value = bytesToObject(valueWrapper.readValue(), Object.class);
        Element e = new Element(valueWrapper.getKey(), value, valueWrapper.getLastUpdateTime(), creation, lastAccessTime,
                valueWrapper.getLastUpdateTime(), hitCount);
        e.setTimeToLive(0);
        e.setTimeToIdle(valueWrapper.getExpiry());
        return e;
    }

    @Override
    public List<Object> getKeys() {
        return new ArrayList<Object>(map.keySet());
    }

    @Override
    public Element remove(Object key) {
        if (key == null) {
            return null;
        }
        Element element = getQuiet(key);
        ValueWrapper valueWrapper = this.map.remove(key);
        return element;
    }

    @Override
    public Element removeWithWriter(Object key, CacheWriterManager writerManager) throws CacheException {
        if (key == null) {
            return null;
        }
        Element removed = remove(key);
        if (writerManager != null) {
            writerManager.remove(new CacheEntry(key, removed));
        }

        if (removed == null && logger.isDebugEnabled()) {
            logger.debug(cache.getName() + "Cache: Cannot remove entry as key " + key + " was not found");
        }

        return removed;
    }

    @Override
    public void removeAll() throws CacheException {
        this.map.clear();
    }

    @Override
    public Element putIfAbsent(Element element) throws NullPointerException {
        Object key = element.getKey();
        if (key == null) {
            return null;
        }
        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            Element e = getQuiet(key);
            ValueWrapper valueWrapper = store(element);
            if (valueWrapper != null) {
                ValueWrapper oldValueWrapper = map.putIfAbsent(key, valueWrapper);
                //TODO: we need read node, but oldValueWrapper is already freed.
                //byte[] oldValue = oldValueWrapper.readValue();
                if (oldValueWrapper != null) {
                    return e;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Element removeElement(Element element, ElementValueComparator comparator)
            throws NullPointerException {
        if (element == null || element.getObjectKey() == null) {
            return null;
        }

        Lock lock = getWriteLock(element.getObjectKey());
        lock.lock();

        try {
            Element toRemove = getQuiet(element.getObjectKey());
            if (comparator.equals(element, toRemove)) {
                remove(element.getObjectKey());
                return toRemove;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Replace the cached element only if the node of the current Element is
     *  equal to the node of the supplied old Element.
     * @param old
     * @param element
     * @param comparator
     * @return
     * @throws NullPointerException
     * @throws IllegalArgumentException
     */
    @Override
    public boolean replace(Element old, Element element, ElementValueComparator comparator)
            throws NullPointerException, IllegalArgumentException {
        if (element == null || element.getObjectKey() == null) {
            return false;
        }

        Lock lock = getWriteLock(element.getKey());
        lock.lock();
        try {
            Element toUpdate = getQuiet(element.getObjectKey());
            if (comparator.equals(old, toUpdate)) {
                put(element);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Replace the cached element only if an Element is currently cached for this key
     * @param element
     *          Element to be cached
     * @return
     *          the Element previously cached for this key, or null if no Element was cached
     * @throws NullPointerException
     */
    @Override
    public Element replace(Element element) throws NullPointerException {
        Lock lock = getWriteLock(element.getKey());
        lock.lock();
        try {
            Element toUpdate = getQuiet(element.getObjectKey());
            if (toUpdate != null) {
                put(element);
                return toUpdate;
            } else {
                return null;
            }
        }finally {
            lock.unlock();
        }
    }

    @Override
    public synchronized void dispose() {
        if (status.equals(Status.STATUS_SHUTDOWN)) {
            return;
        }
        this.map.clear();
        this.allocator.dispose();
        status = Status.STATUS_SHUTDOWN;
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
        long size = map.keySet().size();
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
        //TODO: it is actually not zero
        return 0;
    }

    @Override
    public long getOffHeapSizeInBytes() {
        return this.allocator.used();
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
        return map.containsKey(key);
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
        //to be implemented.
    }

    @Override
    public boolean bufferFull() {
        //never backs up/ no buffer usedMemory.
        //TODO: to be implemented.
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
        if (lockProvider != null) {
            return lockProvider;
        } else {
            lockProvider = new LockProvider();
            return lockProvider;
        }
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
        //TODO: check if has mem space for new element
        if (element == null) {
            return;
        }
        put(element);
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

    private Lock getWriteLock(Object key) {
        return map.lockFor(key).writeLock();
    }

    /**
     * LockProvider implementation that uses the segment locks.
     */
    private class LockProvider implements CacheLockProvider {

        public Sync getSyncForKey(Object key) {
            return new ReadWriteLockSync(map.lockFor(key));
        }
    }

    private byte[] objectToBytes(Object o) {
        try {
            return serializer.serialize(o);
        } catch (IOException e) {
            // should not happen.
            throw new RuntimeException(e);
        }
    }

    private <T> T bytesToObject(byte[] bytes, Class<T> clazz) {
        try {
            return serializer.deserialize(bytes, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
