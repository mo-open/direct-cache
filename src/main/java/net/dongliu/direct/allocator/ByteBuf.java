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

import java.nio.ByteBuffer;

/**
 * A random and sequential accessible sequence of zero or more bytes (octets).
 * This interface provides an abstract view for one or more primitive byte
 * arrays ({@code byte[]}) and {@linkplain ByteBuffer NIO buffers}.
 */
public class ByteBuf extends ReferenceCounted<ByteBuf> {

    PoolChunk chunk;
    long handle;
    // the real used memory size
    int size;
    // the total memory we have
    int capacity;
    //TODO: for cache, mostly not freed by the same thread, thread-local cache not work well
    Thread initThread;
    private long memoryAddress;

    static ByteBuf newInstance() {
        ByteBuf buf = new ByteBuf();
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

    /**
     * Returns the number of bytes (octets) this buffer really have
     */
    public final int size() {
        return size;
    }

    /**
     * Returns the number of bytes (octets) this buffer can contain.
     */
    public final int capacity() {
        return capacity;
    }

    /**
     * Returns the {@link Allocator} which created this buffer.
     */
    public final Allocator alloc() {
        return chunk.arena.parent;
    }

    /**
     * get byte at index i
     */
    public byte get(int i) {
        return UNSAFE.getByte(addr(i));
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex}
     * of this buffer.
     *
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     *                                   if the specified {@code dstIndex} is less than {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.size}, or
     *                                   if {@code dstIndex + length} is greater than
     *                                   {@code dst.length}
     */
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

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     *                                   if the specified {@code srcIndex} is less than {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.size}, or
     *                                   if {@code srcIndex + length} is greater than {@code src.length}
     */
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
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readerIndex} and increases the {@code readerIndex}
     * by the number of the transferred bytes (= {@code dst.length}).
     *
     * @throws IndexOutOfBoundsException if {@code dst.length} is greater than {@code this.readableBytes}
     */
    public ByteBuf readBytes(byte[] dst) {
        getBytes(0, dst, 0, dst.length);
        return this;
    }

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code src.length}).
     *
     * @throws IndexOutOfBoundsException if {@code src.length} is greater than {@code this.writableBytes}
     */
    public ByteBuf writeBytes(byte[] src) {
        setBytes(0, src, 0, src.length);
        return this;
    }

    /**
     * Should be called by every method that tries to access the buffers content to check
     * if the buffer was released before.
     */
    private void ensureAccessible() {
        if (refCnt() == 0) {
            throw new IllegalReferenceCountException(0);
        }
    }

    /**
     * Returns the low-level memory address that point to the first byte of ths backing data.
     *
     * @throws UnsupportedOperationException if this buffer does not support accessing the low-level memory address
     */
    public long memoryAddress() {
        return memoryAddress;
    }

    private long addr(int index) {
        return memoryAddress + index;
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
}
