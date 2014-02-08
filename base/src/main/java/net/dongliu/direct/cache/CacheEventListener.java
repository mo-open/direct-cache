package net.dongliu.direct.cache;

import net.dongliu.direct.struct.ValueWrapper;

/**
 * @author dongliu
 */
public interface CacheEventListener {

    void notifyEvicted(ValueWrapper wrapper);

    void notifyExpired(ValueWrapper wrapper);
}
