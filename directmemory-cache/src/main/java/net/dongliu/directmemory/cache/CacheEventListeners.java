package net.dongliu.directmemory.cache;

import net.dongliu.directmemory.struct.Pointer;

/**
 * @author dongliu
 */
interface CacheEventListener {

    void notifyEvicted(Pointer element, boolean b);

    void notiryExpired(Pointer element, boolean b);
}
