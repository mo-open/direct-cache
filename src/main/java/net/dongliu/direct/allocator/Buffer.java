package net.dongliu.direct.allocator;


import net.dongliu.direct.buffer.ByteBuf;

/**
 * @author Dong Liu
 */
public class Buffer extends AbstractBuffer {
    private final NettyAllocator nettyAllocator;
    private final ByteBuf buf;
    private int size;

    public Buffer(NettyAllocator nettyAllocator, ByteBuf buf) {
        this.nettyAllocator = nettyAllocator;
        this.buf = buf;
    }

    /**
     * release buffers
     */
    @Override
    public void release() {
        nettyAllocator.release(buf);
    }

    @Override
    public int capacity() {
        return buf.capacity();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public byte[] toBytes() {
        byte[] bytes = new byte[size];
        buf.readBytes(bytes);
        return bytes;
    }

    @Override
    public void write(byte[] bytes) {
        this.size = bytes.length;
        buf.writeBytes(bytes);
    }
}
