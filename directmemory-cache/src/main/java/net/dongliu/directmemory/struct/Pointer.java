package net.dongliu.directmemory.struct;

import net.dongliu.directmemory.memory.Allocator;

import java.util.concurrent.atomic.AtomicBoolean;
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

    public final Allocator allocator;

    /** the timestamp when this point constructed. */
    public long created;

    /** expire timestamp */
    public long expiration;

    public long hits;

    public final AtomicLong lastHit;

    private final AtomicBoolean live = new AtomicBoolean();

    private Object key;

    public Pointer(MemoryBuffer memoryBuffer, Allocator allocator) {
        this.memoryBuffer = memoryBuffer;
        this.allocator = allocator;
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

    public MemoryBuffer getMemoryBuffer() {
        return memoryBuffer;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    public long getExpiration() {
        return this.expiration;
    }

    public AtomicBoolean getLive() {
        return live;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public void free() {
        if (this.live.compareAndSet(true, false)) {
            this.allocator.free(this.memoryBuffer);
        }
    }

    public byte[] getValue() {
        return memoryBuffer.read();
    }
}
