package net.dongliu.directmemory.memory.struct;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

/**
 * Wrapper the memoryBuffer, and provide info-fields a cache entry needed.
 *
 * @author dongliu
 */
public class Pointer {

    public final MemoryBuffer memoryBuffer;

    /** the timestamp when this point constructed. */
    public long created;

    /** expire timestamp */
    public long expiration;

    public long hits;

    public final AtomicLong lastHit;

    public Pointer(MemoryBuffer memoryBuffer) {
        this.memoryBuffer = memoryBuffer;
        this.created = System.currentTimeMillis();
        expiration = 0;
        lastHit = new AtomicLong(0);
    }

    public float getFrequency() {
        return (float) (currentTimeMillis() - created) / hits;
    }

    public int getCapacity() {
        return memoryBuffer.getCapacity();
    }

    @Override
    public String toString() {
        return format("Pointer: %s[%s]", getClass().getSimpleName(), getSize());
    }

    public boolean isExpired() {
        return expiration > 0 && expiration < System.currentTimeMillis();
    }

    public int getSize() {
        return memoryBuffer.getSize();
    }

    public void hit() {
        lastHit.set(System.currentTimeMillis());
        hits++;
    }

    public boolean isFree() {
        return expiration == -1;
    }

    public void free() {
        expiration = -1;
    }

    public MemoryBuffer getMemoryBuffer() {
        return memoryBuffer;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    public long getExpiration() {
        return this.expiration;
    }


}
