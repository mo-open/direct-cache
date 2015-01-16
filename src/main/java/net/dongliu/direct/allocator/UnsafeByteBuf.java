/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package net.dongliu.direct.allocator;

import net.dongliu.direct.exception.IllegalReferenceCountException;
import net.dongliu.direct.utils.UNSAFE;

class UnsafeByteBuf extends ReferenceCountedByteBuf {

    PoolChunk chunk;
    long handle;
    // the real used memory size
    int size;
    // the total memory we have
    int capacity;
    //TODO: for cache, mostly not freed by the same thread, thread-local cache not work well
    Thread initThread;
    private long memoryAddress;

    static UnsafeByteBuf newInstance() {
        UnsafeByteBuf buf = new UnsafeByteBuf();
        buf.setRefCnt(1);
        return buf;
    }

    void init(PoolChunk chunk, long handle, int offset, int length, int maxLength) {
        assert handle >= 0;
        assert chunk != null;

        this.chunk = chunk;
        this.handle = handle;
        this.size = length;
        this.capacity = maxLength;
        initThread = Thread.currentThread();
        this.memoryAddress = chunk.memory.getAddress() + offset;
    }

    void initUnpooled(PoolChunk chunk, int length) {
        assert chunk != null;

        this.chunk = chunk;
        handle = 0;
        this.size = capacity = length;
        initThread = Thread.currentThread();
        this.memoryAddress = chunk.memory.getAddress();
    }

    @Override
    public final int size() {
        return size;
    }

    @Override
    public final int capacity() {
        return capacity;
    }

    @Override
    public final Allocator alloc() {
        return chunk.arena.parent;
    }

    @Override
    public byte get(int i) {
        return UNSAFE.getByte(addr(i));
    }

    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            alloc().getUsed().getAndAdd(-capacity());
            final long handle = this.handle;
            this.handle = -1;
            boolean sameThread = initThread == Thread.currentThread();
            initThread = null;
            chunk.arena.free(chunk, handle, capacity, sameThread);
        }
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        checkIndex(index, length);
        if (dst == null) {
            throw new NullPointerException("dst");
        }
        if (dstIndex < 0 || dstIndex > dst.length - length) {
            throw new IndexOutOfBoundsException("dstIndex: " + dstIndex);
        }
        if (length != 0) {
            UNSAFE.copyMemory(addr(index), dst, dstIndex, length);
        }
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        checkIndex(index, length);
        if (length != 0) {
            UNSAFE.copyMemory(src, srcIndex, addr(index), length);
        }
        return this;
    }

    private void checkIndex(int index, int fieldLength) {
        ensureAccessible();
        if (fieldLength < 0) {
            throw new IllegalArgumentException("length: " + fieldLength + " (expected: >= 0)");
        }
        if (index < 0 || index > size() - fieldLength) {
            throw new IndexOutOfBoundsException(String.format(
                    "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, size()));
        }
    }

    /**
     * Should be called by every method that tries to access the buffers content to check
     * if the buffer was released before.
     */
    protected final void ensureAccessible() {
        if (refCnt() == 0) {
            throw new IllegalReferenceCountException(0);
        }
    }

    @Override
    public long memoryAddress() {
        return memoryAddress;
    }

    private long addr(int index) {
        return memoryAddress + index;
    }

}
