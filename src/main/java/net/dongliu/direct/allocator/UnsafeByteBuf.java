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

import net.dongliu.direct.utils.UNSAFE;

class UnsafeByteBuf extends ReferenceCountedByteBuf {

    private final Recycler.Handle recyclerHandle;

    protected PoolChunk chunk;
    protected long handle;
    protected Memory memory;
    protected int offset;
    protected int length;
    int maxLength;
    Thread initThread;
    private long memoryAddress;

    private static final Recycler<UnsafeByteBuf> RECYCLER = new Recycler<UnsafeByteBuf>() {
        @Override
        protected UnsafeByteBuf newObject(Handle handle) {
            return new UnsafeByteBuf(handle, 0);
        }
    };

    static UnsafeByteBuf newInstance(int maxCapacity) {
        UnsafeByteBuf buf = RECYCLER.get();
        buf.setRefCnt(1);
        buf.maxCapacity(maxCapacity);
        return buf;
    }

    protected UnsafeByteBuf(Recycler.Handle recyclerHandle, int maxCapacity) {
        super(maxCapacity);
        this.recyclerHandle = recyclerHandle;
    }

    void init(PoolChunk chunk, long handle, int offset, int length, int maxLength) {
        assert handle >= 0;
        assert chunk != null;

        this.chunk = chunk;
        this.handle = handle;
        memory = chunk.memory;
        this.offset = offset;
        this.length = length;
        this.maxLength = maxLength;
        initThread = Thread.currentThread();
        initMemoryAddress();
    }

    void initUnpooled(PoolChunk chunk, int length) {
        assert chunk != null;

        this.chunk = chunk;
        handle = 0;
        memory = chunk.memory;
        offset = 0;
        this.length = maxLength = length;
        initThread = Thread.currentThread();
        initMemoryAddress();
    }

    private void initMemoryAddress() {
        memoryAddress = memory.getAddress() + offset;
    }

    @Override
    public final int capacity() {
        return length;
    }

    @Override
    public final ByteBuf capacity(int newCapacity) {
        ensureAccessible();
        alloc().getUsed().getAndAdd(-capacity());
        ByteBuf buf = _capacity(newCapacity);
        alloc().getUsed().getAndAdd(buf.capacity());
        return this;
    }

    private ByteBuf _capacity(int newCapacity) {
        // If the request capacity does not require reallocation, just update the length of the memory.
        if (chunk.unpooled) {
            if (newCapacity == length) {
                return this;
            }
        } else {
            if (newCapacity > length) {
                if (newCapacity <= maxLength) {
                    length = newCapacity;
                    return this;
                }
            } else if (newCapacity < length) {
                if (newCapacity > maxLength >>> 1) {
                    if (maxLength <= 512) {
                        if (newCapacity > maxLength - 16) {
                            length = newCapacity;
                            return this;
                        }
                    } else { // > 512 (i.e. >= 1024)
                        length = newCapacity;
                        return this;
                    }
                }
            } else {
                return this;
            }
        }

        // Reallocation required.
        chunk.arena.reallocate(this, newCapacity, true);
        return this;
    }

    @Override
    public final Allocator alloc() {
        return chunk.arena.parent;
    }

    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            alloc().getUsed().getAndAdd(-capacity());
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            boolean sameThread = initThread == Thread.currentThread();
            initThread = null;
            chunk.arena.free(chunk, handle, maxLength, sameThread);
            recycle();
        }
    }

    private void recycle() {
        Recycler.Handle recyclerHandle = this.recyclerHandle;
        if (recyclerHandle != null) {
            ((Recycler<Object>) recycler()).recycle(this, recyclerHandle);
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

    @Override
    public long memoryAddress() {
        return memoryAddress;
    }

    private long addr(int index) {
        return memoryAddress + index;
    }

    protected Recycler<?> recycler() {
        return RECYCLER;
    }

    protected final int idx(int index) {
        return offset + index;
    }
}
