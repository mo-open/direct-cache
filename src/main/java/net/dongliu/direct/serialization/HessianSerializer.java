package net.dongliu.direct.serialization;

import net.dongliu.direct.Serializer;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Default serializer using java object stream.
 *
 * @author dongliu
 */
public final class HessianSerializer implements Serializer {

    @Override
    public <T> void serialize(T value, OutputStream out) throws SerializeException, IOException {
        Hessian2Output oos = new Hessian2Output(out);
        try {
            oos.writeObject(value);
            oos.flush();
        } finally {
            try {
                oos.close();
            } catch (IOException ignore) {
            }
        }
    }

    @Override
    public <T> T deSerialize(InputStream in, Class<T> clazz) throws DeSerializeException {
        Hessian2Input ois = new Hessian2Input(in);
        try {
            return (T) ois.readObject();
        } catch (IOException e) {
            throw new DeSerializeException(e);
        } finally {
            try {
                ois.close();
            } catch (IOException ignore) {
            }
        }
    }
}
