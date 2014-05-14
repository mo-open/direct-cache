package net.dongliu.direct.serialization;

import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author dongliu
 */
public interface ValueSerializer<T> {
    byte[] serialize(T value) throws SerializeException;

    T deserialize(byte[] bytes) throws DeSerializeException;
}
