package net.dongliu.direct.memory.je;

import net.dongliu.direct.memory.UnsafeMemory;

/**
 * @author dongliu
 */
public class PooledByteBuf {
    public int length;
    public PoolChunk chunk;
    public long handle;
    public UnsafeMemory memory;
    public int offset;
    private int maxLength;

    void init(PoolChunk chunk, long handle, int offset, int length, int maxLength) {
        assert handle >= 0;
        assert chunk != null;

        this.chunk = chunk;
        this.handle = handle;
        this.memory = chunk.memory;
        this.offset = offset;
        this.length = length;
        this.maxLength = maxLength;
    }


    public static PooledByteBuf newInstance() {
        return new PooledByteBuf();
    }

    protected final void deallocate() {
        if (handle >= 0) {
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            chunk.arena.free(chunk, handle);
        }
    }
}
