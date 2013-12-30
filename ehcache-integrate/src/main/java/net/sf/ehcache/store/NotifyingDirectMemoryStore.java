package net.sf.ehcache.store;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

public class NotifyingDirectMemoryStore extends DirectMemoryStore {

    private final Ehcache cache;

    private NotifyingDirectMemoryStore(Ehcache cache) {
        super(cache, true);
        this.cache = cache;
    }

    public static NotifyingDirectMemoryStore create(Ehcache cache) {
        NotifyingDirectMemoryStore store = new NotifyingDirectMemoryStore(cache);
        // TODO: implement this.
        //cache.getCacheConfiguration().addConfigurationListener(store);
        return store;
    }

    /**
     * {@inheritDoc}
     * and notifies listeners
     */
    @Override
    protected boolean evict(Element element) {
        Element remove = remove(element.getObjectKey());
        if (remove != null) {
            this.cache.getCacheEventNotificationService().notifyElementEvicted(element, false);
        }
        return remove != null;
    }

    @Override
    protected void notifyDirectEviction(Element element) {
        this.cache.getCacheEventNotificationService().notifyElementEvicted(element, false);
    }

    @Override
    public void expireElements() {
        for (Object key : getKeys()) {
            final Element element = expireElement(key);
            if (element != null) {
                cache.getCacheEventNotificationService().notifyElementExpiry(element, false);
            }
        }
    }
}
