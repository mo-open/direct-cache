package net.sf.ehcache.store;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.pool.Pool;
import net.sf.ehcache.pool.PoolableStore;

public class NotifyingDirectMemoryStore extends DirectMemoryStore {

    private final Ehcache cache;

    private NotifyingDirectMemoryStore(Ehcache cache, Pool<PoolableStore> pool) {
        super(cache, true);
        this.cache = cache;
    }

    public static NotifyingDirectMemoryStore create(Ehcache cache, Pool<PoolableStore> pool) {
        NotifyingDirectMemoryStore store = new NotifyingDirectMemoryStore(cache, pool);
        return store;
    }

    protected boolean evict(Element element) {
        Element remove = remove(element.getObjectKey());
        if (remove != null) {
            this.cache.getCacheEventNotificationService().notifyElementEvicted(element, false);
        }
        return remove != null;
    }

    protected void notifyDirectEviction(Element element) {
        this.cache.getCacheEventNotificationService().notifyElementEvicted(element, false);
    }

    @Override
    public void expireElements() {
        for (Object key : this.getKeys()) {
            //expire element check if it is expired, if it is expired remove from cache and return element
            //.iskeyvalid()
            Element element = remove(key);
            if (element != null) {
                this.cache.getCacheEventNotificationService().notifyElementExpiry(element, false);
            }
        }
    }
}
