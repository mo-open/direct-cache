package net.sf.ehcache.store;

import net.dongliu.directcache.cache.CacheConcurrentHashMap;
import net.dongliu.directcache.cache.CacheEventListener;
import net.dongliu.directcache.exception.AllocatorException;
import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.memory.SlabsAllocator;
import net.dongliu.directcache.serialization.Serializer;
import net.dongliu.directcache.serialization.SerializerFactory;
import net.dongliu.directcache.struct.*;
import net.sf.ehcache.*;
import net.sf.ehcache.concurrent.CacheLockProvider;
import net.sf.ehcache.concurrent.ReadWriteLockSync;
import net.sf.ehcache.concurrent.Sync;
import net.sf.ehcache.event.RegisteredEventListeners;
import net.sf.ehcache.store.disk.StoreUpdateException;
import net.sf.ehcache.writer.CacheWriterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * direct memory store impl.
 * @author dongliu
 */
public class DirectMemoryStore extends AbstractStore implements TierableStore {

    private static Logger logger = LoggerFactory.getLogger(DirectMemoryStore.class);

    private volatile Status status;

    private CacheConcurrentHashMap map;

    private final Allocator allocator;

    private final Ehcache cache;

    private Serializer serializer;

    private volatile CacheLockProvider lockProvider;

    private static final int DEFAULT_CONCURRENCY = 256;

    private static final int INITIAL_CAPACITY = 1000;

    private static final float LOAD_FACTOR = 0.75f;

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

        final RegisteredEventListeners eventListener = doNotifications ? cache.getCacheEventNotificationService() : null;
        CacheEventListener cacheEventListener = null;
        if (eventListener != null) {
            cacheEventListener = new CacheEventListener() {
                @Override
                public void notifyEvicted(ValueWrapper wrapper) {
                    eventListener.notifyElementEvicted(toElement((EhcacheValueWrapper) wrapper), false);
                }

                @Override
                public void notifyExpired(ValueWrapper wrapper) {
                    eventListener.notifyElementEvicted(toElement((EhcacheValueWrapper) wrapper), false);
                }
            };
        }

        this.map = new CacheConcurrentHashMap(allocator, INITIAL_CAPACITY, LOAD_FACTOR, DEFAULT_CONCURRENCY,
                cacheEventListener);

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
        throw new UnsupportedOperationException("pinned element is not support.");
    }

    @Override
    public boolean put(Element element) throws CacheException {
        if (element == null) {
            return true;
        }

        Object key = element.getKey();

        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            ValueWrapper oldValueWrapper = null;
            AbstractValueWrapper valueWrapper = store(element);
            if (valueWrapper != null) {
                oldValueWrapper = map.put(key, valueWrapper);
            } else {
                notifyDirectEviction(element);
                return true;
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
    private EhcacheValueWrapper store(Element element) {
        Object value = element.getValue();
        if (value == null) {
            value = NonHolder.instance;
        }
        byte[] bytes = objectToBytes(value);
        MemoryBuffer buffer;
        try {
            buffer = allocator.allocate(bytes.length);
        } catch (AllocatorException e) {
            logger.error("Allocate new Buffer failed.", e);
            buffer = null;
        }
        if (buffer == null) {
            return null;
        }

        EhcacheValueWrapper wrapper = new EhcacheValueWrapper(buffer);
        buffer.write(bytes);
        wrapper.setKey(element.getKey());
        wrapper.setTimeToIdle(element.getTimeToIdle());
        wrapper.setTimeToLive(element.getTimeToLive());
        wrapper.setHitCount(element.getHitCount());
        wrapper.setVersion(element.getVersion());
        wrapper.setLastUpdateTime(element.getLastUpdateTime());
        wrapper.setCreationTime(element.getCreationTime());
        wrapper.setLastAccessTime(element.getLastAccessTime());
        wrapper.setValueClass(value.getClass());
        return wrapper;
    }

    @Override
    public boolean putWithWriter(Element element, CacheWriterManager writerManager) throws CacheException {
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
     * @param key
     * @return
     */
    @Override
    public Element getQuiet(Object key) {
        EhcacheValueWrapper wrapper = (EhcacheValueWrapper) map.get(key);
        if (wrapper == null) {
            return null;
        }

        // remove expired items.
        if (wrapper.isExpired() || !wrapper.isLive()) {
            Lock lock = getWriteLock(key);
            lock.lock();
            try {
                wrapper = (EhcacheValueWrapper) map.get(key);
                if (wrapper.isExpired() || !wrapper.isLive()) {
                    map.remove(key);
                }
            } finally {
                lock.unlock();
            }
            return null;
        }

        return toElement(wrapper);
    }

    @Override
    public List<Object> getKeys() {
        return new ArrayList<Object>(map.keySet());
    }

    /**
     * Evicts the element for the given key, if it exists and is expired
     * @param key the key
     * @return the evicted element, if any. Otherwise null
     */
    protected Element expireElement(final Object key) {
        Element e = get(key);
        return e != null && e.isExpired() && map.remove(key, e) ? e : null;
    }

    @Override
    public Element remove(Object key) {
        Lock lock = getWriteLock(key);
        lock.lock();
        try {
            Element oldElement = getQuiet(key);
            this.map.remove(key);
            return oldElement;
        } finally {
            lock.unlock();
        }
    }

    private Element toElement(EhcacheValueWrapper wrapper) {
        if (!wrapper.isLive()) {
            //TODO: do someting when wrapper is already dead
        }
        Object value;
        if (wrapper.getValueClass() == NonHolder.class) {
            value = null;
        } else {
            value = bytesToObject(wrapper.readValue(), wrapper.getValueClass());
        }
        Element e = new Element(wrapper.getKey(), value, wrapper.getVersion(),
                wrapper.getCreationTime(), wrapper.getLastAccessTime(),
                wrapper.getLastUpdateTime(), wrapper.getHitCount());
        e.setTimeToLive(wrapper.getTimeToLive());
        e.setTimeToIdle(wrapper.getTimeToIdle());
        return e;
    }

    @Override
    public Element removeWithWriter(Object key, CacheWriterManager writerManager) throws CacheException {
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
        if (element == null) {
            return null;
        }
        Object key = element.getKey();

        Lock lock = getWriteLock(element.getObjectKey());
        lock.lock();
        try {
            Element oldElement = getQuiet(key);
            if (oldElement != null) {
                return oldElement;
            }
            ValueWrapper valueWrapper = store(element);
            if (valueWrapper != null) {
                map.put(key, valueWrapper);
                return null;
            }else {
                notifyDirectEviction(element);
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Evicts the element from the store
     * @param element the element to be evicted
     * @return true if succeeded, false otherwise
     */
    protected boolean evict(Element element){
        final Element remove = remove(element.getObjectKey());
        RegisteredEventListeners cacheEventNotificationService = cache.getCacheEventNotificationService();
        final FrontEndCacheTier frontEndCacheTier = cacheEventNotificationService.getFrontEndCacheTier();
        if (remove != null && frontEndCacheTier != null && frontEndCacheTier.notifyEvictionFromCache(remove.getKey())) {
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
        if (element == null || element.getObjectKey() == null) {
            return null;
        }
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

    public Collection<Element> elementSet() {
        // TODO: to be implemented.
        return Collections.emptyList();
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
        if (o instanceof NonHolder) {
            return new byte[0];
        }
        try {
            return serializer.serialize(o);
        } catch (IOException e) {
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
