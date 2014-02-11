package net.dongliu.direct.cache;

import net.dongliu.direct.struct.ValueHolder;

/**
 * @author dongliu
 */
public interface CacheEventListener {

    void notifyEvicted(ValueHolder wrapper);

    void notifyExpired(ValueHolder wrapper);
}
