package net.dongliu.direct.memory.slabs;

import net.dongliu.direct.memory.MemoryBuffer;
import net.dongliu.direct.memory.UnsafeMemory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A collection of same chunk size slab.
 *
 * @author dongliu
 */
class SlabClass {
    protected final int chunkSize;
    private final List<Slab> slabList;
    private volatile Slab curSlab;

    private final ConcurrentLinkedQueue<Chunk> freeChunkQueue;
    private final Object expandLock = new Object();

    public SlabClass(int chunkSize) {
        this.chunkSize = chunkSize;
        this.slabList = new ArrayList<Slab>();
        this.freeChunkQueue = new ConcurrentLinkedQueue<Chunk>();
    }

    /**
     * tryKill a chunk. lock tryKill
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

            if (newSlab() == null) {
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
    private Slab newSlab() {
        UnsafeMemory memory = UnsafeMemory.allocate(this.chunkSize);
        MemoryBuffer buffer = new MemoryBuffer(memory, 0, this.chunkSize);
        curSlab = Slab.make(buffer, chunkSize);
        slabList.add(curSlab);
        return curSlab;
    }
}

