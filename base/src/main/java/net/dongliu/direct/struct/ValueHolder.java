package net.dongliu.direct.struct;

/**
 * interface of cache-value holder.
 *
 * @author dongliu
 */
public interface ValueHolder {

    int getCapacity();

    boolean isExpired();

    int getSize();

    boolean isLive();

    Object getKey();

    void setKey(Object key);

    /**
     * release resources and mark as not live.
     */
    void dispose();

    /**
     * read value in bytes. this should only be called once
     */
    byte[] readValue();

}
