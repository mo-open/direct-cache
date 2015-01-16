package net.dongliu.direct.allocator;

import java.io.IOException;
import java.io.InputStream;

/**
 * wrap byte buf as input stream
 *
 * @author Dong Liu
 */
public class ByteBufInputStream extends InputStream {

    /**
     * the byte buf used as input stream
     */
    private final ByteBuf buf;

    /**
     * The index of the next character to read from the byte buf
     */
    private int pos;

    /**
     * The currently marked position in the stream.
     */
    protected int mark = 0;

    /**
     * The index one greater than the last valid character in the input stream buffer.
     */
    protected int end;

    /**
     * Creates a ByteBufInputStream so that it  uses buf as its buffer array.
     */
    public ByteBufInputStream(ByteBuf buf) {
        this.buf = buf.retain();
        this.pos = 0;
        this.end = buf.size();
    }

    /**
     * Creates ByteBufInputStream
     */
    public ByteBufInputStream(ByteBuf buf, int offset, int length) {
        this.buf = buf.retain();
        this.pos = offset;
        this.end = Math.min(offset + length, buf.size());
        this.mark = offset;
    }

    @Override
    public synchronized int read() {
        return (pos < end) ? (buf.get(pos++) & 0xff) : -1;
    }

    @Override
    public synchronized int read(byte b[], int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        if (pos >= end) {
            return -1;
        }

        int avail = end - pos;
        if (len > avail) {
            len = avail;
        }
        if (len <= 0) {
            return 0;
        }
        buf.getBytes(pos, b, off, len);
        pos += len;
        return len;
    }

    @Override
    public synchronized long skip(long n) {
        long k = end - pos;
        if (n < k) {
            k = n < 0 ? 0 : n;
        }

        pos += k;
        return k;
    }

    @Override
    public synchronized int available() {
        return end - pos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readAheadLimit) {
        mark = pos;
    }

    @Override
    public synchronized void reset() {
        pos = mark;
    }

    @Override
    public void close() throws IOException {
        buf.release();
    }

}
