package net.dongliu.direct.struct;

import net.dongliu.direct.utils.U;

/**
 * Wrapper the memoryBuffer, and provide info-fields a cache entry needed.
 *
 * @author dongliu
 */
public abstract class AbstractValueWrapper implements ValueWrapper {

    private Object key;

    public final MemoryBuffer memoryBuffer;

    private volatile int live;

    private static final long LIVE_OFFSET;

    static {
        LIVE_OFFSET = U.objectFieldOffset(AbstractValueWrapper.class, "live");
    }

    public AbstractValueWrapper(MemoryBuffer memoryBuffer) {
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
    public MemoryBuffer getMemoryBuffer() {
        return this.memoryBuffer;
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

    /**
     * return buffer to allocator.
     *
     * @return false if pointer has already been freed.
     */
    public boolean tryKill() {
        return U.compareAndSwapInt(this, LIVE_OFFSET, 1, 0);
    }

}
