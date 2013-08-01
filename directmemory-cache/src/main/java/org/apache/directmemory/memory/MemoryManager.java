package org.apache.directmemory.memory;

import java.io.Closeable;
import java.util.Set;

/**
 * Store, retrive, update and remove object with a memory buffer.
 * @param <V>
 */
public interface MemoryManager<V> extends Closeable {

    /**
     * Initialize the internal structure. Need to be called before the service can be used.
     *
     * @param numberOfBuffers : number of internal bucket
     * @param size            : size in B of internal buckets
     */
    void init(int numberOfBuffers, int size);

    /**
     * Store function family. Store the given payload at a certain offset in a MemoryBuffer, returning the pointer to
     * the value.
     *
     * @param payload : the data to store
     * @param expiresIn : expire time.
     * @return the pointer to the value, or null if not enough space has been found.
     */
    Pointer<V> store(byte[] payload, long expiresIn);

    /**
     * Same function as {@link #store(byte[])}, but add an relative expiration delta in milliseconds
     *
     * @param payload   : the data to store
     * @return the pointer to the value, or null if not enough space has been found.
     */
    Pointer<V> store(byte[] payload);

    /**
     * Update value of a {@link Pointer}
     *
     * @param pointer
     * @param payload
     * @return
     * @throw BufferOverflowException if the size of the payload id bigger than the pointer capacity
     */
    Pointer<V> update(Pointer<V> pointer, byte[] payload);

    byte[] retrieve(Pointer<V> pointer);

    Pointer<V> free(Pointer<V> pointer);

    void clear();

    long capacity();

    long used();

    long collectExpired();

    void collectLFU();

    <T extends V> Pointer<V> allocate(Class<T> type, int size, long expiresIn, long expires);

    Set<Pointer<V>> getPointers();

    <T extends V> Pointer<V> allocate(Class<T> type, int size);
}
