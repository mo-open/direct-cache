/*
 * Copyright 2013 The Netty Project
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

import java.util.Objects;

/**
 * An empty {@link ByteBuf} whose size and maximum size are all {@code 0}.
 */
public final class EmptyByteBuf extends ByteBuf {

    private static final Memory EMPTY_BYTE_BUFFER = UNSAFE.allocateMemory(0);
    private static final long EMPTY_BYTE_BUFFER_ADDRESS = EMPTY_BYTE_BUFFER.getAddress();

    private final Allocator alloc;
    private final String str;

    public EmptyByteBuf(Allocator alloc) {
        Objects.requireNonNull(alloc, "alloc should not be null");
        this.alloc = alloc;
        str = this.getClass().getSimpleName();
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public Allocator alloc() {
        return alloc;
    }

    @Override
    public byte get(int i) {
        throw new ArrayIndexOutOfBoundsException();
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        return _checkIndex(index, length);
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        return _checkIndex(index, length);
    }

    @Override
    public ByteBuf readBytes(byte[] dst) {
        return checkLength(dst.length);
    }

    @Override
    public ByteBuf writeBytes(byte[] src) {
        return checkLength(src.length);
    }

    @Override
    public long memoryAddress() {
        return EMPTY_BYTE_BUFFER_ADDRESS;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ByteBuf && ((ByteBuf) obj).size() == 0;
    }

    @Override
    public String toString() {
        return str;
    }

    @Override
    public int refCnt() {
        return 1;
    }

    @Override
    public ByteBuf retain() {
        return this;
    }

    @Override
    public ByteBuf retain(int increment) {
        return this;
    }

    @Override
    public boolean release() {
        return false;
    }

    @Override
    public boolean release(int decrement) {
        return false;
    }

    private ByteBuf _checkIndex(int index, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length: " + length);
        }
        if (index != 0 || length != 0) {
            throw new IndexOutOfBoundsException();
        }
        return this;
    }

    private ByteBuf checkLength(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length: " + length + " (expected: >= 0)");
        }
        if (length != 0) {
            throw new IndexOutOfBoundsException();
        }
        return this;
    }
}
