package net.dongliu.direct.struct;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.utils.U;

/**
 * Wrapper the memoryBuffer, and provide info-fields a cache entry needed, to store cache values.
 *
 * @author dongliu
 */
public abstract class BufferValueHolder implements ValueHolder {

    private Object key;

    public final MemoryBuffer memoryBuffer;

    private volatile int live;

    private static final long LIVE_OFFSET;

    static {
        LIVE_OFFSET = U.objectFieldOffset(BufferValueHolder.class, "live");
    }

    public BufferValueHolder(MemoryBuffer memoryBuffer) {
        this.memoryBuffer = memoryBuffer;
        this.live = 1;
    }

    @Override
    public boolean isLive() {
        return this.live == 1;
    }

    @Override
    public int getCapacity() {
        return memoryBuffer.getCapacity();
    }

    @Override
    public int getSize() {
        return memoryBuffer.getSize();
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public void setKey(Object key) {
        this.key = key;
    }

    @Override
    public byte[] readValue() {
        return memoryBuffer.read();
    }

    @Override
    public void dispose() {
        if (U.compareAndSwapInt(this, LIVE_OFFSET, 1, 0)) {
            this.memoryBuffer.dispose();
        }
    }

    public void markDead() {
        U.compareAndSwapInt(this, LIVE_OFFSET, 1, 0);
    }
}
