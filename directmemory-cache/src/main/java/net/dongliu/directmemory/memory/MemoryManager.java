package net.dongliu.directmemory.memory;

import net.dongliu.directmemory.memory.struct.Pointer;

import java.io.Closeable;
import java.util.Date;
import java.util.Set;

/**
 * Store, retrive, update and remove object with a memory buffer.
 * @author dongliu
 */
public interface MemoryManager extends Closeable {

    /**
     * Initialize the internal structure. Need to be called before the service can be usedMemory.
     *
     * @param size            : size in B of internal buckets
     */
    void init(int size);


    /**
     * Store function family. Store the given payload at a certain offset in a MemoryBuffer, returning the pointer to
     * the value.
     * @param payload   : the data to store
     * @return the pointer to the value, or null if not enough space has been found.
     */
    Pointer store(byte[] payload);

    /**
     * Same function as {@link #store(byte[])}, but add an relative expiration delta in milliseconds
     *
     * @param payload : the data to store
     * @param expiresIn : expire in expiresIn milliseconds.
     * @return the pointer to the value, or null if not enough space has been found.
     */
    Pointer store(byte[] payload, long expiresIn);

    /**
     * Same function as {@link #store(byte[])}, but add an relative expiration delta in milliseconds
     *
     * @param payload : the data to store
     * @param expirestill : store till expiresTill time.
     * @return the pointer to the value, or null if not enough space has been found.
     */
    Pointer store(byte[] payload, Date expirestill);

    /**
     * Update value of a {@link Pointer}
     *
     * @return new point if payload is larger than origin payload, null if failed, else the same poiter passed in.
     */
    Pointer update(Pointer pointer, byte[] payload);

    /**
     * Update value of a {@link Pointer}
     *
     * @return new point if payload is larger than origin payload, null if failed, else the same poiter passed in.
     */
    Pointer update(Pointer pointer, byte[] payload, long expiresIn);

    /**
     * Update value of a {@link Pointer}
     *
     * @return new point if payload is larger than origin payload, null if failed, else the same poiter passed in.
     */
    Pointer update(Pointer pointer, byte[] payload, Date expirestill);

    byte[] retrieve(Pointer pointer);

    /**
     * set the entry's expireIn time.
     * @param pointer could not be null
     * @param expiresIn milliseconds
     */
    void expire(Pointer pointer, long expiresIn);

    /**
     * set the entry's expireIn time.
     * @param pointer could not be null
     * @param expirestill expire time.
     */
    void expire(Pointer pointer, Date expirestill);

    void free(Pointer pointer);

    void clear();

    long capacity();

    long used();

    void collectExpired();

    void collectLFU();

    Set<Pointer> getPointers();

}
