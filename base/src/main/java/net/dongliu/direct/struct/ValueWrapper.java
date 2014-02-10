package net.dongliu.direct.struct;

/**
 * interface of value holder.
 *
 * @author dongliu
 */
public interface ValueWrapper {

    int getCapacity();

    boolean isExpired();

    int getSize();

    boolean isLive();

    MemoryBuffer getMemoryBuffer();

    Object getKey();

    void setKey(Object key);

    /**
     * mark value wrapper to be dead if it is alive.
     *
     * @return true if value wrapper is alive before.
     */
    boolean tryKill();

    /**
     * read value in bytes. this should only be called once
     */
    byte[] readValue();

}
