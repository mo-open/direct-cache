package net.dongliu.direct.memory.slabs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A collection of same chunk size slab.
 *
 * @author  Dong Liu
 */
class SlabClass {
    protected final int chunkSize;
    private final List<Slab> slabList;
    private volatile Slab curSlab;

    protected final SlabsAllocator allocator;
    private final ConcurrentLinkedQueue<Chunk> freeChunkQueue;
    private final Object expandLock = new Object();

    public SlabClass(SlabsAllocator allocator, int chunkSize) {
        this.allocator = allocator;
        this.chunkSize = chunkSize;
        this.slabList = new ArrayList<>();
        this.freeChunkQueue = new ConcurrentLinkedQueue<>();
    }

    /**
     * tryKill a chunk. lock tryKill
     */
    public void freeChunk(Chunk chunk) {
        this.allocator.actualUsed.addAndGet(-chunkSize);
        this.freeChunkQueue.add(chunk);
    }

    /**
     * get a chunk.
     */
    public Chunk newChunk() {

        // only was null when first request to this SlabClass
        if (this.curSlab == null) {
            synchronized (this.expandLock) {
                if (this.curSlab == null) {
                    newSlab();
                }
            }
        }

        Chunk chunk = this.freeChunkQueue.poll();
        if (chunk != null) {
            return chunk;
        }

        chunk = this.curSlab.nextChunk();
        if (chunk != null) {
            return chunk;
        }

        synchronized (this.expandLock) {
            // try again. curSlab may have changed.
            chunk = this.curSlab.nextChunk();
            if (chunk != null) {
                return chunk;
            }

            newSlab();
            if (this.curSlab == null) {
                return null;
            }

            chunk = this.curSlab.nextChunk();
            return chunk;
        }
    }

    /**
     * allocate one more slab.
     * thread-safe by atomic. memroy was hold by multi SlabClass, so thead-safe is required.
     * Note: curSlab & slabList was modified as a side-effect.
     */
    private void newSlab() {
        if (this.allocator.used.addAndGet(this.allocator.chunkSize) < this.allocator.capacity) {
            this.curSlab = Slab.newInstance(this, this.allocator.slabSize, this.chunkSize);
            this.slabList.add(this.curSlab);
        } else {
            this.allocator.used.addAndGet(-this.allocator.chunkSize);
        }
    }

    public void destroy() {
        for (Slab slab : this.slabList) {
            slab.destroy();
        }
        this.slabList.clear();
        this.freeChunkQueue.clear();
    }
}

