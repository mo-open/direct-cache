package net.dongliu.direct.serialization;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Default serializer using java object stream.
 *
 * @author dongliu
 */
public final class DefaultSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(T value) throws SerializeException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Hessian2Output oos = new Hessian2Output(baos);
        try {
            oos.writeObject(value);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            //should not happen
            throw new SerializeException(e);
        } finally {
            try {
                oos.close();
            } catch (IOException ignore) {
            }
        }
    }

    @Override
    public T deSerialize(byte[] bytes, Class<T> clazz) throws DeSerializeException {
        Hessian2Input ois = new Hessian2Input(new ByteArrayInputStream(bytes));
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
