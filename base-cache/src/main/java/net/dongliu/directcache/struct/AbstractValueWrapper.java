package net.dongliu.directcache.struct;

import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.utils.U;

/**
 * Wrapper the memoryBuffer, and provide info-fields a cache entry needed.
 *
 * @author dongliu
 */
public abstract class AbstractValueWrapper implements ValueWrapper {

    private Object key;

    public final MemoryBuffer memoryBuffer;

    private volatile int live = 1;


    private static final long LIVE_OFFSET;

    static {
        LIVE_OFFSET = U.objectFieldOffset(AbstractValueWrapper.class, "live");
    }

    public AbstractValueWrapper(MemoryBuffer memoryBuffer) {
        this.memoryBuffer = memoryBuffer;
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
        return memoryBuffer;
    }

    @Override
    public boolean isLive() {
        return live == 1;
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public void setKey(Object key) {
        this.key = key;
    }

    /**
     * return buffer to allocator.
     * @return false if pointer has already been freed.
     */
    @Override
    public boolean returnTo(final Allocator allocator) {
        if (U.compareAndSwapInt(this, LIVE_OFFSET, 1, 0)) {
            allocator.free(this.memoryBuffer);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public byte[] readValue() {
        return memoryBuffer.read();
    }


}
