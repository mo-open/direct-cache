package net.dongliu.direct.struct;

/**
 * the value in bytes
 *
 * @author Dong Liu
 */
public class BytesValue<T> {

    private final byte[] bytes;

    public BytesValue(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
