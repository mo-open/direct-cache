package net.dongliu.direct.serialization;

import net.dongliu.direct.utils.IOUtils;

import java.io.*;

/**
 * Default serializer using java object stream.
 *
 * @author dongliu
 */
public final class DefaultSerializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        try {
            oos.writeObject(obj);
            oos.flush();
        } finally {
            IOUtils.closeQueitly(oos);
        }
        return bos.toByteArray();
    }

    @Override
    public Object deserialize(byte[] source)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(source);
        ObjectInputStream ois = new ObjectInputStream(bis);
        try {
            return ois.readObject();
        } finally {
            IOUtils.closeQueitly(ois);
        }
    }

}
