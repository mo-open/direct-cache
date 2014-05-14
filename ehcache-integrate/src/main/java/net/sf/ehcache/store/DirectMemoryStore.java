package net.sf.ehcache.store;

import net.dongliu.direct.cache.CacheEventListener;
import net.dongliu.direct.cache.ConcurrentMap;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.slabs.SlabsAllocator;
import net.dongliu.direct.serialization.DefaultSerializer;
import net.dongliu.direct.serialization.ValueSerializer;
import net.dongliu.direct.struct.EhcacheDummyValueHolder;
import net.dongliu.direct.struct.EhcacheValueHolder;
import net.dongliu.direct.struct.ValueHolder;
import net.dongliu.direct.utils.CacheConfigure;
import net.sf.ehcache.*;
import net.sf.ehcache.concurrent.CacheLockProvider;
import net.sf.ehcache.concurrent.ReadWriteLockSync;
import net.sf.ehcache.concurrent.Sync;
import net.sf.ehcache.event.RegisteredEventListeners;
import net.sf.ehcache.store.disk.StoreUpdateException;
import net.sf.ehcache.writer.CacheWriterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * direct memory store impl.
 *
 * @author dongliu
 */
public class DirectMemoryStore extends AbstractStore implements TierableStore {

    private static Logger logger = LoggerFactory.getLogger(DirectMemoryStore.class);

    private volatile Status status;

    private ConcurrentMap map;

    private final Allocator allocator;

    private final Ehcache cache;

    private ValueSerializer<Object> serializer;

    private volatile CacheLockProvider lockProvider;

    public static DirectMemoryStore create(Ehcache cache) {
        return new DirectMemoryStore(cache, false);
    }

    protected DirectMemoryStore(Ehcache cache, boolean doNotifications) {
        this.status = Status.STATUS_UNINITIALISED;
        // we have checked the offHeapBytes setting before create DirectMemoryStore.
        // assume it's all right here.
        long offHeapSizeBytes = cache.getCacheConfiguration().getMaxMemoryOffHeapInBytes();
        this.cache = cache;

        this.allocator = SlabsAllocator.newInstance(offHeapSizeBytes);

        final RegisteredEventListeners eventListener =
                doNotifications ? cache.getCacheEventNotificationService() : null;
        CacheEventListener cacheEventListener = null;
        if (eventListener != null) {
            cacheEventListener = new CacheEventListener() {
                @Override
                public void notifyEvicted(ValueHolder holder) {
                    eventListener.notifyElementEvicted(toElement((EhcacheValueHolder) holder), false);
                }

                @Override
                public void notifyExpired(ValueHolder holder) {
                    eventListener.notifyElementEvicted(toElement((EhcacheValueHolder) holder), false);
                }
            };
        }

        CacheConfigure cc = CacheConfigure.getConfigure();
        this.map = new ConcurrentMap(cc.getInitialSize(), cc.getLoadFactor(), cc.getConcurrency(),
                cacheEventListener);

        this.serializer = new DefaultSerializer();
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
        throw new UnsupportedOperationException("pinned element is not support.");
    }

    @Override
    public boolean put(Element element) throws CacheException {
        return put(element, true);
    }

    private boolean put(Element element, boolean doEvict) {
        if (element == null) {
            return true;
        }

        Object key = element.getKey();
        ValueHolder holder = store(element);

        if (doEvict && holder == null) {
            //map.evictEntries(key);
            holder = store(element);
        }

        ValueHolder oldValueHolder;
        if (holder != null) {
            oldValueHolder = map.put(key, holder);
        } else {
            map.remove(key);
            notifyDirectEviction(element);
            return true;
        }
        return oldValueHolder == null;
    }

    /**
     * allocate memory and store the payload, return the pointer.
     *
     * @return the point.null if failed.
     */
    private EhcacheValueHolder store(Element element) {
        Object value = element.getValue();
        EhcacheValueHolder holder;
        if (value != null) {
            MemoryBuffer buffer = objectToBuffer(value);
            if (buffer == null) {
                logger.debug("Allocate new Buffer failed.");
                return null;
            }

            holder = new EhcacheValueHolder(buffer);
        } else {
            holder = EhcacheDummyValueHolder.newNullValueHolder();
        }
        holder.setKey(element.getKey());
        holder.setTimeToIdle(element.getTimeToIdle());
        holder.setTimeToLive(element.getTimeToLive());
        holder.setHitCount(element.getHitCount());
        holder.setVersion(element.getVersion());
        holder.setLastUpdateTime(element.getLastUpdateTime());
        holder.setCreationTime(element.getCreationTime());
        holder.setLastAccessTime(element.getLastAccessTime());
        return holder;
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
        return getQuiet(key);
    }

    /**
     * Gets an Element from the Store, without updating statistics.
     */
    @Override
    public Element getQuiet(Object key) {
        Lock readLock = readLock(key);
        readLock.lock();
        EhcacheValueHolder holder;
        try {
            holder = (EhcacheValueHolder) map.get(key);
            if (holder == null) {
                return null;
            } else {
                holder.acquire();
            }
        } finally {
            readLock.unlock();
        }

        try {
            if (!holder.isExpired()) {
                return toElement(holder);
            }
        } finally {
            holder.release();
        }

        // remove expired item.
        Lock lock = writeLock(key);
        lock.lock();
        try {
            EhcacheValueHolder newHolder = (EhcacheValueHolder) map.get(key);
            if (newHolder != null && newHolder.isExpired()) {
                map.remove(key);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    @Override
    public List<Object> getKeys() {
        return new ArrayList<Object>(map.keySet());
    }

    /**
     * Evicts the element for the given key, if it exists and is expired
     *
     * @param key the key
     * @return the evicted element, if any. Otherwise null
     */
    protected Element expireElement(final Object key) {
        Element e = get(key);
        return e != null && e.isExpired() && map.remove(key, e) ? e : null;
    }

    @Override
    public Element remove(Object key) {
        Lock lock = writeLock(key);
        lock.lock();
        try {
            Element oldElement = getQuiet(key);
            this.map.remove(key);
            return oldElement;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Element removeWithWriter(Object key, CacheWriterManager writerManager)
            throws CacheException {
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
    public Element putIfAbsent(Element element) {
        if (element == null) {
            return null;
        }
        Object key = element.getKey();
        Lock lock = writeLock(key);
        lock.lock();
        try {
            Element oldElement = getQuiet(key);
            if (oldElement == null) {
                put(element);
            }
            return oldElement;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Evicts the element from the store
     *
     * @param element the element to be evicted
     * @return true if succeeded, false otherwise
     */
    protected boolean evict(Element element) {
        final Element remove = remove(element.getObjectKey());
        RegisteredEventListeners cacheEventNotificationService = cache.getCacheEventNotificationService();
        final FrontEndCacheTier frontEndCacheTier = cacheEventNotificationService.getFrontEndCacheTier();
        if (remove != null && frontEndCacheTier != null
                && frontEndCacheTier.notifyEvictionFromCache(remove.getKey())) {
            cacheEventNotificationService.notifyElementEvicted(remove, false);
        }
        return remove != null;
    }

    /**
     * Called when an element is evicted even before it could be installed inside the store
     *
     * @param element the evicted element
     */
    protected void notifyDirectEviction(Element element) {
    }

    @Override
    public Element removeElement(Element element, ElementValueComparator comparator)
            throws NullPointerException {
        if (element == null || element.getObjectKey() == null) {
            return null;
        }

        Lock lock = writeLock(element.getObjectKey());
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

    @Override
    public boolean replace(Element old, Element element, ElementValueComparator comparator)
            throws NullPointerException, IllegalArgumentException {
        if (element == null || element.getObjectKey() == null) {
            return false;
        }

        Lock lock = writeLock(element.getKey());
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
     *
     * @param element Element to be cached
     * @return the Element previously cached for this key, or null if no Element was cached
     * @throws NullPointerException
     */
    @Override
    public Element replace(Element element) throws NullPointerException {
        if (element == null || element.getObjectKey() == null) {
            return null;
        }
        Lock lock = writeLock(element.getKey());
        lock.lock();
        try {
            Element toUpdate = getQuiet(element.getObjectKey());
            if (toUpdate != null) {
                put(element);
                return toUpdate;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public synchronized void dispose() {
        if (status.equals(Status.STATUS_SHUTDOWN)) {
            return;
        }
        this.map.clear();
        this.allocator.destroy();
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
        return 0;
    }

    @Override
    public long getOffHeapSizeInBytes() {
        return this.allocator.actualUsed();
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

    /**
     * Expire all elements.
     * <p/>
     * This is a default implementation which does nothing. Expiration on demand is only implemented for disk stores.
     */
    @Override
    public void expireElements() {
        for (Object key : map.keySet()) {
            expireElement(key);
        }
    }

    /**
     * Flush to disk only if the cache is diskPersistent.
     */
    @Override
    public void flush() {
        if (cache.getCacheConfiguration().isClearOnFlush()) {
            removeAll();
        }
    }

    @Override
    public boolean bufferFull() {
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

// -------------------------- implements TierableStore ---------------------------------------

    @Override
    public void fill(Element element) {
        if (element == null) {
            return;
        }
        put(element, false);
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

    // -------------------------- util methods ---------------------------------------

    private Lock writeLock(Object key) {
        return map.lockFor(key).writeLock();
    }

    private Lock readLock(Object key) {
        return map.lockFor(key).readLock();
    }

    /**
     * LockProvider implementation that uses the segment locks.
     */
    private class LockProvider implements CacheLockProvider {

        public Sync getSyncForKey(Object key) {
            return new ReadWriteLockSync(map.lockFor(key));
        }
    }

    private Element toElement(EhcacheValueHolder holder) {
        Object value;
        if (holder instanceof EhcacheDummyValueHolder) {
            value = null;
        } else {
            value = bufferToObject(holder.getMemoryBuffer());
        }
        Element e = new Element(holder.getKey(), value, holder.getVersion(),
                holder.getCreationTime(), holder.getLastAccessTime(),
                holder.getLastUpdateTime(), holder.getHitCount());
        e.setTimeToLive(holder.getTimeToLive());
        e.setTimeToIdle(holder.getTimeToIdle());
        return e;
    }

    /**
     * write object to byte buffer
     *
     * @param o not null
     * @return
     */
    private MemoryBuffer objectToBuffer(Object o) {
        byte[] bytes;
        try {
            bytes = serializer.serialize(o);
        } catch (SerializeException e) {
            throw new CacheException("serialize value failed", e);
        }
        MemoryBuffer buffer = this.allocator.allocate(bytes.length);
        if (buffer == null) {
            return null;
        }
        buffer.write(bytes);
        return buffer;
    }

    /**
     * deserialize mem buffer to object
     *
     * @param buffer not null
     * @return
     */
    private Object bufferToObject(MemoryBuffer buffer) {
        try {
            return serializer.deserialize(buffer.toBytes());
        } catch (DeSerializeException e) {
            throw new CacheException("deserialize value failed", e);
        }
    }
}
