package net.dongliu.direct;

import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

import java.io.*;

/**
 * Default serializer use java serialization
 *
 * @author Dong Liu
 */
public class DefaultSerializer implements Serializer {
    @Override
    public <T> void serialize(T value, OutputStream out) throws SerializeException, IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(value);
        }
    }

    @Override
    public <T> T deSerialize(InputStream in, Class<T> clazz) throws DeSerializeException, IOException {
        try (ObjectInputStream ois = new ObjectInputStream(in)) {
            try {
                return (T) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new DeSerializeException(e);
            }
        }
    }
}
