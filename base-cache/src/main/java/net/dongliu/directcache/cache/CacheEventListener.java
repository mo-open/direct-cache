package net.dongliu.directcache.cache;

import net.dongliu.directcache.struct.ValueWrapper;

/**
 * @author dongliu
 */
public interface CacheEventListener {

    void notifyEvicted(ValueWrapper element, boolean b);

    void notifyExpired(ValueWrapper element, boolean b);
}
