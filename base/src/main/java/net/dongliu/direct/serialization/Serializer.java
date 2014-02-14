package net.dongliu.direct.serialization;

/**
 * serializer interface.
 *
 * @author dongliu
 */
public interface Serializer {

    byte[] serialize(Object obj) throws Exception;

    Object deserialize(byte[] source) throws Exception;

}
