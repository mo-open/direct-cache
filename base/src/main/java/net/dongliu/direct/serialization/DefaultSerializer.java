package net.dongliu.direct.serialization;

import net.dongliu.direct.utils.IOUtils;

import java.io.*;

/**
 * Default serializer using java object stream.
 * @author dongliu
 */
public final class DefaultSerializer implements Serializer {

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize(T obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        try {
            oos.writeObject(obj);
            oos.flush();
        } finally {
            IOUtils.closeQueitly(oos);
        }
        return baos.toByteArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize(byte[] source, final Class<T> clazz) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(source);
        ObjectInputStream ois = new ObjectInputStream(bis);
        try {
            T obj = clazz.cast(ois.readObject());
            return obj;
        } finally {
            IOUtils.closeQueitly(ois);
        }
    }

}
