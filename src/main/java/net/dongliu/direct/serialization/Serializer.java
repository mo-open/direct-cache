package net.dongliu.direct.serialization;

import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

/**
 * serialize value to bytes / deSerialize bytes to value
 *
 * @author Dong Liu
 */
public interface Serializer<T> {

    /**
     * serialize value to bytes
     *
     * @param value the value to serialize. not null
     * @return the bytes
     * @throws SerializeException
     */
    byte[] serialize(T value) throws SerializeException;

    /**
     * deSerialize bytes to value
     *
     * @param bytes the bytes to deSerialize, not null
     * @param clazz the class of the value
     * @return the value
     * @throws DeSerializeException
     */
    T deSerialize(byte[] bytes, Class<T> clazz) throws DeSerializeException;
}
