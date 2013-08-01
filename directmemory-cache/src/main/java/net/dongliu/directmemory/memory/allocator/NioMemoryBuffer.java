package net.dongliu.directmemory.memory.allocator;

import net.dongliu.directmemory.memory.buffer.AbstractMemoryBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

abstract class NioMemoryBuffer
        extends AbstractMemoryBuffer {

    private final ByteBuffer byteBuffer;

    NioMemoryBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public long capacity() {
        return byteBuffer.limit();
    }

    @Override
    public long maxCapacity() {
        return byteBuffer.capacity();
    }

    @Override
    public ByteOrder byteOrder() {
        return byteBuffer.order();
    }

    @Override
    public void byteOrder(ByteOrder byteOrder) {
        byteBuffer.order(byteOrder);
    }

    @Override
    public void clear() {
        byteBuffer.clear();
        byteBuffer.rewind();
        writerIndex = 0;
        readerIndex = 0;
    }

    @Override
    public boolean readable() {
        return byteBuffer.remaining() > 0;
    }

    @Override
    public void readerIndex(long readerIndex) {
        super.readerIndex(readerIndex);
        byteBuffer.position((int) readerIndex);
    }

    @Override
    public void writerIndex(long writerIndex) {
        super.writerIndex(writerIndex);
        byteBuffer.position((int) writerIndex);
    }

    @Override
    public int readBytes(byte[] bytes, int offset, int length) {
        byteBuffer.get(bytes, offset, length);
        readerIndex += length;
        return length;
    }

    @Override
    protected byte readByte(long offset) {
        return byteBuffer.get((int) offset);
    }

    @Override
    public boolean writable() {
        return byteBuffer.position() < byteBuffer.capacity();
    }

    @Override
    protected void writeByte(long offset, byte value) {
        byteBuffer.put((int) offset, value);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int length) {
        byteBuffer.put(bytes, offset, length);
        writerIndex += length;
    }

    protected ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

}
