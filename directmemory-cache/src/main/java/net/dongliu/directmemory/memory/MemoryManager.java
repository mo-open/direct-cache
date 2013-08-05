package net.dongliu.directmemory.memory;

import net.dongliu.directmemory.memory.allocator.Allocator;
import net.dongliu.directmemory.memory.allocator.SlabsAllocator;
import net.dongliu.directmemory.memory.struct.MemoryBuffer;
import net.dongliu.directmemory.memory.struct.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * MemoryManager, wrap Allocators.
 * Store, retrieve, update and remove object with a memory buffer.
 * we do not do synchronizing here, but on the BinaryCache.
 * @author dongliu
 */
public class MemoryManager {

    private static final Logger logger = LoggerFactory.getLogger(MemoryManager.class);

    private final Allocator allocator;

    public MemoryManager(final long size) {
        super();
        this.allocator = SlabsAllocator.getSlabsAllocator(size);
    }

    public Pointer store(byte[] payload) {
        MemoryBuffer buffer = allocator.allocate(payload.length);
        if (buffer == null) {
            return null;
        }

        Pointer p = makePointer(buffer);
        buffer.write(payload);
        return p;
    }

    //TODO: all store with expires need to be synchronized.
    public Pointer store(byte[] payload, long expiresIn) {
        Pointer pointer = store(payload);
        if (pointer == null) {
            return pointer;
        }
        expire(pointer, expiresIn);
        return pointer;
    }

    public Pointer store(byte[] payload, Date expiresTill) {
        Pointer pointer = store(payload);
        if (pointer == null) {
            return pointer;
        }
        expire(pointer, expiresTill);
        return pointer;
    }

    //TODO: all update method need to be synchronized.
    public Pointer update(Pointer pointer, byte[] payload) {
        if (pointer.getCapacity() >= payload.length) {
            pointer.getMemoryBuffer().write(payload);
            pointer.hit();
            return pointer;
        }

        // free first, the value is set to null if update failed.
        free(pointer);
        return store(payload);
    }

    public Pointer update(Pointer pointer, byte[] payload, long expiresIn) {
        Pointer newPointer = update(pointer, payload);
        if (newPointer == null) {
            return newPointer;
        }
        expire(newPointer, expiresIn);
        return newPointer;
    }

    public Pointer update(Pointer pointer, byte[] payload, Date expirestill) {
        Pointer newPointer = update(pointer, payload);
        if (newPointer == null) {
            return newPointer;
        }
        expire(newPointer, expirestill);
        return newPointer;
    }

    public byte[] retrieve(final Pointer pointer) {
        // check if pointer has not been freed before
        pointer.hit();
        final MemoryBuffer buf = pointer.getMemoryBuffer();
        return buf.read();
    }

    public void free(final Pointer pointer) {
        if (pointer.getLive().compareAndSet(true, false)) {
            this.allocator.free(pointer.getMemoryBuffer());
        }
    }

    public long capacity() {
        return allocator.getCapacity();
    }

    public long used() {
        return this.allocator.used();
    }

    public void close() throws IOException {
        allocator.close();
    }

    private Pointer makePointer(final MemoryBuffer buffer) {
        return new Pointer(buffer);
    }

    public long free(Iterable<Pointer> pointers) {
        long howMuch = 0;
        for (Pointer expired : pointers) {
            howMuch += expired.getCapacity();
            free(expired);
        }
        return howMuch;
    }

    public void expire(Pointer pointer, long expiresIn) {
        pointer.setExpiration(pointer.created + expiresIn);
    }

    public void expire(Pointer pointer, Date expirestill) {
        pointer.setExpiration(expirestill.getTime());
    }
}
