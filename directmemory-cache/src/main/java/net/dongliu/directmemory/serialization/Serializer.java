package net.dongliu.directmemory.serialization;

import java.io.IOException;

/**
 * <b>All implementations must be thread-safe</b>
 */
public interface Serializer {

    <T> byte[] serialize(T obj)
            throws IOException;

    <T> T deserialize(byte[] source, Class<T> clazz)
            throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException;

}
