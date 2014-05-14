package net.dongliu.direct.serialization;

import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author dongliu
 */
public interface ValueSerializer<T> {
    void writeObject(T value, OutputStream out) throws SerializeException;

    T readValue(InputStream in) throws DeSerializeException;
}
