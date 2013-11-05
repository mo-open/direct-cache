package net.dongliu.directcache.memory;

import net.dongliu.directcache.struct.MemoryBuffer;
import net.dongliu.directcache.utils.Ram;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory Allocator use slabList, as memcached.
 * TODO: slab rebalance.
 * @author dongliu
 */
public class SlabsAllocator implements Allocator {

    /** chunk size expand factor */
    private static final float expandFactor = 1.25f;

    /** minum size of chunk */
    private static final int CHUNK_SIZE = 128;

    /** 8M. it is also the max Chunk Size */
    private static final int SLAB_SIZE = Ram.Mb(8);

    private static final int[] CHUNK_SIZE_LIST;

    private static final int CHUNK_SIZE_MASK = ~3;

    static {
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

    private final MergedMemory mergedMemory;

    private final AtomicLong used;
    private SlabsAllocator(long capacity) {
        this.mergedMemory = MergedMemory.allocate(capacity);
        this.used = new AtomicLong(0);

        slabClasses = new SlabClass[CHUNK_SIZE_LIST.length];
        for (int i = 0; i < CHUNK_SIZE_LIST.length; i++) {
            SlabClass slabClass = new SlabClass(mergedMemory, CHUNK_SIZE_LIST[i]);
            slabClasses[i] = slabClass;
        }
    }

    public static SlabsAllocator getSlabsAllocator(long size) {
        return new SlabsAllocator(size);
    }

    @Override
    public MemoryBuffer allocate(int size) {
        SlabClass slabClass = locateSlabClass(size);
        if (slabClass == null) {
            return null;
        }
        Chunk chunk = slabClass.newChunk();
        if (chunk != null) {
            used.addAndGet(chunk.getCapacity());
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
            } while(this.slabClasses[mid].chunkSize < size);
            return this.slabClasses[mid];
        } else {
            do {
                mid--;
            } while( mid >= 0 && this.slabClasses[mid].chunkSize > size);
            return this.slabClasses[mid + 1];
        }
    }

    @Override
    public void free(MemoryBuffer memoryBuffer) {
        if (memoryBuffer.isDispose()) {
            return;
        }
        Chunk chunk = (Chunk)memoryBuffer;
        SlabClass slabClass = locateSlabClass(chunk.getSize());
        slabClass.freeChunk(chunk);
        this.used.addAndGet(-chunk.getCapacity());
    }

    @Override
    public long getCapacity() {
        return mergedMemory.capacity();
    }

    @Override
    public long used() {
        return this.used.longValue();
    }

    @Override
    public void dispose() {
        this.mergedMemory.dispose();
        for (int i = 0; i < this.slabClasses.length; i++) {
            this.slabClasses[i] = null;
        }
        this.used.set(0);
    }

    /**
     * A collection of same chunk size slab.
     */
    private static class SlabClass {
        private final int chunkSize;
        private final List<Slab> slabList;
        private volatile Slab curSlab;

        private final ConcurrentLinkedQueue<Chunk> freeChunkQueue;
        private final MergedMemory memory;
        private final Object expandLock = new Object();

        public SlabClass (MergedMemory memory, int chunkSize) {
            this.memory = memory;
            this.chunkSize = chunkSize;
            this.slabList = new ArrayList<Slab>();
            this.freeChunkQueue = new ConcurrentLinkedQueue<Chunk>();
        }

        /**
         * returnTo a chunk. lock returnTo
         */
        public void freeChunk(Chunk chunk) {
            freeChunkQueue.add(chunk);
        }

        /**
         * get a chunk.
         */
        public Chunk newChunk() {

            // only was null when first request to this SlabClass
            if (curSlab == null) {
                synchronized (expandLock) {
                    if (curSlab == null) {
                        newSlab(this.chunkSize);
                    }
                }
            }

            Chunk chunk = freeChunkQueue.poll();
            if (chunk != null) {
                return chunk;
            }

            chunk = curSlab.nextChunk();
            if (chunk != null) {
                return chunk;
            }

            synchronized (expandLock) {
                // try again. curSlab may have changed.
                chunk = curSlab.nextChunk();
                if (chunk != null) {
                    return chunk;
                }

                if (newSlab(this.chunkSize) == null) {
                    return null;
                }

                chunk = curSlab.nextChunk();
                return chunk;
            }
        }

        /**
         * allocate one more slab.
         * thread-safe by atomic. memroy was hold by multi SlabClass, so thead-safe is required.
         * Note: curSlab & slabList was modified as a side-effect.
         */
        private Slab newSlab(int chunkSize) {
            MemoryBuffer buffer = memory.malloc(chunkSize);
            if (buffer == null) {
                return null;
            }
            curSlab = Slab.make(buffer, chunkSize);
            slabList.add(curSlab);
            return curSlab;
        }
    }

    /**
     * slab.
     */
    private static class Slab extends MemoryBuffer {

        private final AtomicInteger idx = new AtomicInteger(0);

        private final int chunkSize;

        private Slab(MergedMemory memory, long start, int capacity, int chunkSize) {
            super(memory, start, capacity);
            this.chunkSize = chunkSize;
        }

        public static Slab make(MemoryBuffer buffer, int chunkSize) {
            return new Slab(buffer.getMemory(), buffer.getStart(), buffer.getCapacity(), chunkSize);
        }

        /**
         * return next chunk in this slab.
         * @return null if have no chunk left.
         */
        public Chunk nextChunk() {
            int freeChunkIdx = this.idx.getAndIncrement();
            int total = this.getCapacity() / chunkSize;
            if (freeChunkIdx < total) {
                return Chunk.make(this, freeChunkIdx * chunkSize, chunkSize);
            }
            this.idx.getAndDecrement();
            return null;
        }
    }

    /**
     * chunk
     */
    private static class Chunk extends MemoryBuffer {

        public static Chunk make(Slab slab, int start, int capacity) {
            return new Chunk(slab, start, capacity);
        }

        private Chunk(Slab slab, int start, int capacity) {
            this(slab.getMemory(), slab.getStart() + start, capacity);
        }

        private Chunk(MergedMemory byteBuffer, long start, int size) {
            super(byteBuffer, start, size);
        }
    }

}
