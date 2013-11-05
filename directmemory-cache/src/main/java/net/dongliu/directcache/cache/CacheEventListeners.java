package net.dongliu.directcache.cache;

import net.dongliu.directcache.struct.Pointer;

/**
 * @author dongliu
 */
interface CacheEventListener {

    void notifyEvicted(Pointer element, boolean b);

    void notiryExpired(Pointer element, boolean b);
}
