package net.dongliu.direct;

import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * serialize value to bytes / deSerialize bytes to value
 *
 * @author Dong Liu
 */
public interface Serializer {

    /**
     * serialize value to bytes
     *
     * @param value the value to serialize. not null
     * @throws SerializeException
     */
    <T> void serialize(T value, OutputStream out) throws SerializeException, IOException;

    /**
     * deSerialize bytes to value
     *
     * @param in    the input stream to deSerialize
     * @param clazz the class of the value
     * @return the value
     * @throws DeSerializeException
     */
    <T> T deSerialize(InputStream in, Class<T> clazz) throws DeSerializeException, IOException;
}
