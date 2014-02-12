package net.dongliu.direct.struct;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.utils.U;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wrapper the memoryBuffer, and provide info-fields a cache entry needed, to store cache values.
 *
 * @author dongliu
 */
public abstract class BufferValueHolder implements ValueHolder {

    private Object key;

    public final MemoryBuffer memoryBuffer;

    private final AtomicInteger count;

    public BufferValueHolder(MemoryBuffer memoryBuffer) {
        this.memoryBuffer = memoryBuffer;
        this.count = new AtomicInteger(1);
    }

    @Override
    public boolean isLive() {
        return this.count.intValue() > 0;
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
        // this guard is not thread-safe
        if (count.intValue() <=0) {
            throw new IllegalArgumentException("This buffer has been disposed.");
        }
        return memoryBuffer.read();
    }

    @Override
    public void release() {
        if (count.decrementAndGet() == 0) {
            this.memoryBuffer.dispose();
        }
    }

    @Override
    public void acquire() {
        if (count.incrementAndGet() <= 1) {
            count.decrementAndGet();
            throw new IllegalArgumentException("This buffer has been disposed.");
        }
    }
}
