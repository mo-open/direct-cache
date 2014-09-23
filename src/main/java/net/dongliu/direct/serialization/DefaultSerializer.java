package net.dongliu.direct.serialization;

import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

import java.io.*;

/**
 * Default serializer using java object stream.
 *
 * @author dongliu
 */
public final class DefaultSerializer implements ValueSerializer<Object> {


    @Override
    public byte[] serialize(Object value) throws SerializeException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            //should not happen
            throw new SerializeException(e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws DeSerializeException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new DeSerializeException(e);
        }
    }
}
