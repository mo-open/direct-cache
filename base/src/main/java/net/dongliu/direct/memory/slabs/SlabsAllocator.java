package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.exception.AllocatorException;
import net.dongliu.direct.memory.Allocator;
import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.utils.CacheConfigure;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory Allocator use slabList, as memcached.
 * TODO: slab rebalance.
 *
 * @author dongliu
 */
public class SlabsAllocator implements Allocator {

    /**
     * chunk size expand factor
     */
    private static final float expandFactor;

    /**
     * minum size of chunk
     */
    protected static final int CHUNK_SIZE;

    /**
     * 8M. it is also the max Chunk Size
     */
    protected static final int SLAB_SIZE;

    protected static final int[] CHUNK_SIZE_LIST;

    private static final int CHUNK_SIZE_MASK = ~3;

    static {
        CacheConfigure cc = CacheConfigure.getConfigure();
        expandFactor = cc.getExpandFactor();
        CHUNK_SIZE = cc.getMinEntySize();
        SLAB_SIZE = cc.getMaxEntrySize();

        double size = CHUNK_SIZE;
        List<Integer> ilist = new ArrayList<Integer>();
        while ((((int) size) & CHUNK_SIZE_MASK) <= SLAB_SIZE) {
            ilist.add((((int) size) & CHUNK_SIZE_MASK));
            size = size * expandFactor;
        }

        CHUNK_SIZE_LIST = new int[ilist.size()];
        for (int i = 0; i < ilist.size(); i++) {
            CHUNK_SIZE_LIST[i] = ilist.get(i);
        }
    }

    private final SlabClass[] slabClasses;

    protected final long capacity;

    protected final AtomicLong used = new AtomicLong(0);
    /**
     * offheap memory size had been actualUsed.
     */
    private final AtomicLong actualUsed = new AtomicLong(0);

    private SlabsAllocator(long capacity) {
        this.capacity = capacity;
        slabClasses = new SlabClass[CHUNK_SIZE_LIST.length];
        for (int i = 0; i < CHUNK_SIZE_LIST.length; i++) {
            SlabClass slabClass = new SlabClass(this, CHUNK_SIZE_LIST[i]);
            slabClasses[i] = slabClass;
        }
    }

    public static SlabsAllocator newInstance(long size) {
        return new SlabsAllocator(size);
    }

    @Override
    public MemoryBuffer allocate(int size) throws AllocatorException {

        if (size <= 0) {
            throw new IllegalArgumentException("size must be large than 0");
        }

        SlabClass slabClass = locateSlabClass(size);
        if (slabClass == null) {
            throw new AllocatorException("Data size larger than: " + CHUNK_SIZE_LIST[CHUNK_SIZE_LIST.length]);
        }
        Chunk chunk = slabClass.newChunk();
        if (chunk != null) {
            actualUsed.addAndGet(chunk.getCapacity());
        }
        return chunk;
    }

    /**
     * locate the slabClass index with right size.
     */
    private SlabClass locateSlabClass(int size) {
        int left = 0;
        int right = this.slabClasses.length - 1;

        if (size > this.slabClasses[right].chunkSize) {
            return null;
        }

        // binary search.
        int mid = 0;
        while (left <= right) {
            mid = left + (right - left) / 2;
            if (size > this.slabClasses[mid].chunkSize) {
                left = mid + 1;
            } else if (size < this.slabClasses[mid].chunkSize) {
                right = mid - 1;
            } else {
                return this.slabClasses[mid];
            }
        }

        // chose one from left & right
        if (this.slabClasses[mid].chunkSize < size) {
            do {
                mid++;
            } while (this.slabClasses[mid].chunkSize < size);
            return this.slabClasses[mid];
        } else {
            do {
                mid--;
            } while (mid >= 0 && this.slabClasses[mid].chunkSize > size);
            return this.slabClasses[mid + 1];
        }
    }

    @Override
    public void free(MemoryBuffer memoryBuffer) {
        if (memoryBuffer.isDispose()) {
            return;
        }
        if (memoryBuffer.getCapacity() == 0) {
            return;
        }
        Chunk chunk = (Chunk) memoryBuffer;
        SlabClass slabClass = locateSlabClass(chunk.getSize());
        slabClass.freeChunk(chunk);
        this.actualUsed.addAndGet(-chunk.getCapacity());
    }

    @Override
    public long getCapacity() {
        return this.capacity;
    }

    @Override
    public long actualUsed() {
        return this.actualUsed.longValue();
    }

    @Override
    public void destroy() {
        for (int i = 0; i < this.slabClasses.length; i++) {
            this.slabClasses[i] = null;
        }
        this.actualUsed.set(0);
    }

}