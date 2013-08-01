package net.dongliu.directmemory.memory;

import net.dongliu.directmemory.measures.Ram;
import net.dongliu.directmemory.memory.allocator.Allocator;
import net.dongliu.directmemory.memory.allocator.MergingByteBufferAllocator;
import net.dongliu.directmemory.memory.buffer.MemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.String.format;

/**
 * Default implement of MemoryManager.
 * Store, retrive, update and remove object with a memory buffer.
 * @author dongliu
 */
public class MemoryManagerImpl extends AbstractMemoryManager implements MemoryManager {

    private static final Logger logger = LoggerFactory.getLogger(MemoryManagerImpl.class);

    private Allocator allocator;

    public MemoryManagerImpl() {
        super();
    }

    @Override
    public void init(int size) {
        this.allocator = getByteBufferAllocator(size);
        logger.info(format("MemoryManager initialized - size : %s", Ram.inMb(size)));
    }

    @Override
    public void close() throws IOException {
        allocator.close();
        usedMemory.set(0);
    }

    private Allocator getByteBufferAllocator(final int size) {
        return new MergingByteBufferAllocator(size);
    }

    public Pointer store(byte[] payload) {
        MemoryBuffer buffer = allocator.allocate(payload.length);
        if (buffer == null) {
            return null;
        }

        Pointer p = instanciatePointer(buffer);
        buffer.writerIndex(0);
        buffer.writeBytes(payload);

        usedMemory.addAndGet(payload.length);

        return p;
    }

    @Override
    public Pointer update(Pointer pointer, byte[] payload) {
        if (pointer.getCapacity() >= payload.length) {
            pointer.getMemoryBuffer().writeBytes(payload);
            pointer.hit();
            return pointer;
        }

        // free first, the value is set to null if update failed.
        free(pointer);
        return store(payload);
    }

    @Override
    public byte[] retrieve(final Pointer pointer) {
        // check if pointer has not been freed before
        if (!pointers.contains(pointer)) {
            return null;
        }

        pointer.hit();

        final MemoryBuffer buf = pointer.getMemoryBuffer();
        buf.readerIndex(0);

        final byte[] swp = new byte[(int) buf.readableBytes()];
        buf.readBytes(swp);
        return swp;
    }

    @Override
    public void free(final Pointer pointer) {
        if (!pointers.remove(pointer)) {
            return;
        }
        this.allocator.free(pointer.getMemoryBuffer());
        usedMemory.addAndGet(-pointer.getCapacity());
    }

    @Override
    public long capacity() {
        return allocator.getCapacity();
    }

    @Override
    public void clear() {
        pointers.clear();
        allocator.clear();
        usedMemory.set(0L);
    }

    private Pointer instanciatePointer(final MemoryBuffer buffer) {
        Pointer p = new Pointer(buffer);
        pointers.add(p);
        return p;
    }

}
