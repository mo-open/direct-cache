package net.dongliu.directmemory.memory.allocator;

import net.dongliu.directmemory.memory.struct.MemoryBuffer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory Allocator use slabList, as memcached.
 * @author dongliu
 */
public class SlabsAllocator implements Allocator {

    private static final float expandFactor = 1.25f;
    private static final int CLASS_NUM = 59;

    /** 16byte. */
    private static final int CHUNK_SIZE = 2 << 4;
    /** 8M. it is also the max Chunk Size */
    private static final int SLAB_SIZE = 1<<23;

    private static final int CHUNK_SIZE_MASK = ~3;

    private final SlabClass[] slabClasses = new SlabClass[CLASS_NUM];

    private final MergedMemory mergedMemory;

    private SlabsAllocator(long capacity) {
        mergedMemory = MergedMemory.allocate(capacity);

        float chunkSize = CHUNK_SIZE;
        for (int i = 0; i < CLASS_NUM; i++) {
            SlabClass slabClass = new SlabClass(mergedMemory, ((int) chunkSize) & CHUNK_SIZE_MASK);
            slabClasses[i] = slabClass;
            chunkSize *= expandFactor;
        }
    }

    public static SlabsAllocator getSlabsAllocator(long size) {
        return new SlabsAllocator(size);
    }

    @Override
    public void free(MemoryBuffer memoryBuffer) {
        Chunk chunk = (Chunk)memoryBuffer;
        SlabClass slabClass = locateSlabClass(chunk.getSize());
        slabClass.freeChunk(chunk);
    }

    @Override
    public MemoryBuffer allocate(int size) {
        SlabClass slabClass = locateSlabClass(size);
        if (slabClass == null) {
            return null;
        }
        return slabClass.newChunk();
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
    public long getCapacity() {
        return mergedMemory.capacity();
    }

    @Override
    public long used() {
        return this.mergedMemory.getPosition().longValue();
    }

    @Override
    public void close() {
        this.mergedMemory.close();
        for (int i = 0; i < this.slabClasses.length; i++) {
            this.slabClasses[i] = null;
        }
    }

    private static class SlabClass {
        private final int chunkSize;
        private final int perslab;
        private final List<Slab> slabList;
        private volatile Slab curSlab;
        // ConcurrentLinkedQueue is good, but it cost too much extra space for each node
        private final ConcurrentLinkedQueue<Chunk> freeChunkQueue;
        private final MergedMemory memory;
        private final Object expandLock = new Object();

        public SlabClass (MergedMemory memory, int chunkSize) {
            this.memory = memory;
            this.chunkSize = chunkSize;
            this.perslab = SLAB_SIZE / chunkSize;
            this.slabList = new ArrayList<Slab>();
            this.freeChunkQueue = new ConcurrentLinkedQueue<Chunk>();
        }

        /**
         * free a chunk. lock free
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
                        newSlab();
                    }
                }
            }

            Chunk chunk = freeChunkQueue.poll();
            if (chunk != null) {
                return chunk;
            }

            Slab slab = curSlab;
            AtomicInteger idx = slab.getIdx();
            int newIdx = idx.incrementAndGet();
            if (newIdx <= perslab) {
                chunk = slab.newChunk(newIdx - 1, chunkSize);
                return chunk;
            }

            synchronized (expandLock) {
                // try again.
                if (curSlab.getIdx().intValue() < perslab) {
                    chunk = curSlab.newChunk(curSlab.getIdx().intValue(), chunkSize);
                    curSlab.getIdx().incrementAndGet();
                    return chunk;
                }

                if (newSlab() == null) {
                    return null;
                }

                chunk = curSlab.newChunk(0, chunkSize);
                curSlab.getIdx().set(1);
                return chunk;
            }
        }

        /**
         * allocate one more slab.
         * thread-safe by atomic. memroy was hold by multi SlabClass, so thead-safe is required.
         * Note: curSlab & slabList was modified as a side-effect.
         */
        public Slab newSlab() {
            AtomicLong pos = memory.getPosition();
            long newPos = pos.addAndGet(SLAB_SIZE);
            if (newPos > memory.capacity()) {
                pos.addAndGet(-SLAB_SIZE);
                return null;
            }

            curSlab = Slab.make(memory, newPos - SLAB_SIZE, SLAB_SIZE);
            slabList.add(curSlab);
            return curSlab;
        }
    }

    /**
     * slab.
     */
    private static class Slab extends MemoryBuffer {

        private final AtomicInteger idx = new AtomicInteger(0);

        private Slab(MergedMemory memory, long start, int size) {
            super(memory, start, size);
        }

        public static Slab make(MergedMemory memory, long start, int size) {
            return new Slab(memory, start, size);
        }

        public Chunk newChunk(int freeChunkIdx, int chunkSize) {
            return Chunk.make(this, freeChunkIdx * chunkSize, chunkSize);
        }

        private AtomicInteger getIdx() {
            return idx;
        }
    }

    /**
     * chunk
     */
    private static class Chunk extends MemoryBuffer {

        public static Chunk make(Slab slab, int start, int size) {
            return new Chunk(slab, start, size);
        }

        private Chunk(Slab slab, int start, int size) {
            this(slab.getMemory(), slab.getStart() + start, size);
        }

        private Chunk(MergedMemory byteBuffer, long start, int size) {
            super(byteBuffer, start, size);
        }
    }

}
