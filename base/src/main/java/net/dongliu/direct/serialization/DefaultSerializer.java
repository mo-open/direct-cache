package net.dongliu.direct.serialization;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
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
    public void writeObject(Object value, OutputStream out) throws SerializeException {
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(value);
            oos.flush();
        } catch (IOException e) {
            throw new SerializeException(e);
        }
    }

    @Override
    public Object readValue(InputStream in) throws DeSerializeException {
        try (ObjectInputStream ois = new ObjectInputStream(in)) {
            return ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new DeserializationException(e);
        }
    }
}
