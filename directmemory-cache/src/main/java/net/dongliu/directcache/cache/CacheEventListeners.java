package net.dongliu.directcache.cache;

import net.dongliu.directcache.struct.ValueWrapper;

/**
 * @author dongliu
 */
interface CacheEventListener {

    void notifyEvicted(ValueWrapper element, boolean b);

    void notiryExpired(ValueWrapper element, boolean b);
}
