package net.dongliu.direct.value;

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
