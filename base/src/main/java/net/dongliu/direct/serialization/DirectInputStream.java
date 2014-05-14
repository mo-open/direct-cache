package net.dongliu.direct.serialization;

import net.dongliu.direct.memory.MemoryBuffer;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author dongliu
 */
public class DirectInputStream extends InputStream {

    private int offset;
    private MemoryBuffer buffer;

    public DirectInputStream(MemoryBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() {
        return buffer.read(offset++);
    }

    @Override
    public int read(byte b[], int off, int len) {
        int read = buffer.read(b, off, offset, len);
        this.offset += read;
        return read;
    }

    @Override
    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }
        int size = buffer.getSize();
        if (n + offset > size) {
            n = size - offset;
        }
        this.offset += n;
        return n;
    }

    @Override
    public int available() {
        return this.buffer.getSize() - this.offset;
    }

    @Override
    public void close() {
    }

}
