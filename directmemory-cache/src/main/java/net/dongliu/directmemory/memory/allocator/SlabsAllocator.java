package net.dongliu.directmemory.memory.allocator;

import net.dongliu.directmemory.memory.struct.MemoryBuffer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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

    public static SlabsAllocator allocate(long size) {
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
    public void close() {
        //
    }

    private static class SlabClass {
        private final int chunkSize;                 /* sizes of items */
        private final int perslab;                   /* how many items per slab */
        private final List<Slab> slabList;           /* list of slabList */
        private int nextFreeChunkIdx = -1;           /* idx to next free item at end of page, or -1 */
        private final Queue<Chunk> freeChunkQueue;   /* array of slab pointers */
        private final MergedMemory memory;

        public SlabClass (MergedMemory memory, int chunkSize) {
            this.memory = memory;
            this.chunkSize = chunkSize;
            this.perslab = SLAB_SIZE / chunkSize;
            this.slabList = new ArrayList<Slab>();
            this.freeChunkQueue = new LinkedList<Chunk>();
        }
        /**
         * allocate one more slab.
         */
        public Slab newSlab() {
            long pos = memory.getPosition();
            if (pos + SLAB_SIZE > memory.capacity()) {
                return null;
            }

            Slab slab = Slab.make(memory, pos, SLAB_SIZE);
            memory.setPosition(pos + SLAB_SIZE);
            nextFreeChunkIdx = 0;
            slabList.add(slab);
            return slab;
        }

        /**
         * free a chunk.
         */
        public void freeChunk(Chunk chunk) {
            freeChunkQueue.add(chunk);
        }

        /**
         * get a chunk.
         */
        public Chunk newChunk() {
            Chunk chunk = freeChunkQueue.poll();
            if (chunk != null) {
                return chunk;
            }

            if (nextFreeChunkIdx == -1) {
                Slab slab = newSlab();
                if (slab == null) {
                    return null;
                }
            }

            Slab slab = slabList.get(slabList.size() - 1);
            chunk = slab.newChunk(nextFreeChunkIdx, chunkSize);
            if (++nextFreeChunkIdx == perslab) {
                nextFreeChunkIdx = -1;
            }
            return chunk;

        }
    }

    /**
     * slab.
     */
    private static class Slab extends MemoryBuffer {

        private Slab(MergedMemory memory, long start, int size) {
            super(memory, start, size);
        }

        public static Slab make(MergedMemory memory, long start, int size) {
            return new Slab(memory, start, size);
        }

        public Chunk newChunk(int freeChunkIdx, int chunkSize) {
            return Chunk.make(this, freeChunkIdx * chunkSize, chunkSize);
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
