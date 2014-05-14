package net.dongliu.direct.serialization;

import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author dongliu
 */
//TODO: do not use local temp bytes
public class DirectOutputStream extends OutputStream {

    private ByteArrayOutputStream baos;
    private Allocator allocator;

    public DirectOutputStream(Allocator allocator) {
        this.baos = new ByteArrayOutputStream(128);
        this.allocator = allocator;
    }

    public void write(byte b[], int off, int len) {
        baos.write(b, off, len);
    }


    @Override
    public void write(int b) throws IOException {
        baos.write(b);
    }

    public MemoryBuffer getMemoryBuffer() {
        byte[] data = baos.toByteArray();
        MemoryBuffer buffer = allocator.allocate(data.length);
        buffer.write(data);
        return buffer;
    }
}
