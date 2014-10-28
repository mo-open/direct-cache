package net.dongliu.direct.struct;

/**
 * the value in bytes
 *
 * @author Dong Liu
 */
public class BytesValue<T> {

    private final Class<T> clazz;
    private final byte[] bytes;

    public BytesValue(byte[] bytes, Class<T> clazz) {
        this.clazz = clazz;
        this.bytes = bytes;
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
