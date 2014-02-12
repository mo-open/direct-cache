package net.dongliu.direct.struct;

/**
 * interface of cache-value holder.
 *
 * @author dongliu
 */
public interface ValueHolder {

    /**
     * the off heap capcity
     */
    int getCapacity();

    /**
     * the value have expired
     */
    boolean isExpired();

    /**
     * the offheap buffer size actual used
     */
    int getSize();

    Object getKey();

    void setKey(Object key);

    /**
     * read value in bytes. this should only be called once
     */
    byte[] readValue();

    /**
     * if is still alive.
     */
    boolean isLive();


    // this two methods is for refrence count, to make sure dispose after no more used.

    /**
     * call when need to hold this value.
     */
    void release();

    /**
     * call when no longer used. if the reference count is descrease to 0, it is disposed.
     */
    void acquire();
}
