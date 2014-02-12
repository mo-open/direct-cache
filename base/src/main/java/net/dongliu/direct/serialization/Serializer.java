package net.dongliu.direct.serialization;

/**
 * <b>All implementations must be thread-safe</b>
 */
public interface Serializer {

    <T> byte[] serialize(T obj) throws Exception;

    <T> T deserialize(byte[] source, Class<T> clazz) throws Exception;

}
