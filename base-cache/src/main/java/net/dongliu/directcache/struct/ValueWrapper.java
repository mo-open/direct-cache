package net.dongliu.directcache.struct;

import net.dongliu.directcache.memory.Allocator;

/**
 * interface of value holder.
 * @author dongliu
 */
public interface ValueWrapper {
    int getCapacity();

    boolean isExpired();

    int getSize();

    MemoryBuffer getMemoryBuffer();

    boolean isLive();

    Object getKey();

    void setKey(Object key);

    boolean returnTo(Allocator allocator);

    byte[] readValue();

}
