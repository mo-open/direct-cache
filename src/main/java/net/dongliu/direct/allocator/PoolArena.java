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

import java.nio.ByteBuffer;

abstract class PoolArena {

    static final int numTinySubpagePools = 512 >>> 4;

    final ByteBufAllocator parent;

    private final int maxOrder;
    final int pageSize;
    final int pageShifts;
    final int chunkSize;
    final int subpageOverflowMask;
    final int numSmallSubpagePools;
    private final PoolSubpage[] tinySubpagePools;
    private final PoolSubpage[] smallSubpagePools;

    private final PoolChunkList q050;
    private final PoolChunkList q025;
    private final PoolChunkList q000;
    private final PoolChunkList qInit;
    private final PoolChunkList q075;
    private final PoolChunkList q100;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    protected PoolArena(ByteBufAllocator parent, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        this.parent = parent;
        this.pageSize = pageSize;
        this.maxOrder = maxOrder;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        subpageOverflowMask = ~(pageSize - 1);
        tinySubpagePools = newSubpagePoolArray(numTinySubpagePools);
        for (int i = 0; i < tinySubpagePools.length; i++) {
            tinySubpagePools[i] = newSubpagePoolHead(pageSize);
        }

        numSmallSubpagePools = pageShifts - 9;
        smallSubpagePools = newSubpagePoolArray(numSmallSubpagePools);
        for (int i = 0; i < smallSubpagePools.length; i++) {
            smallSubpagePools[i] = newSubpagePoolHead(pageSize);
        }

        q100 = new PoolChunkList(this, null, 100, Integer.MAX_VALUE);
        q075 = new PoolChunkList(this, q100, 75, 100);
        q050 = new PoolChunkList(this, q075, 50, 100);
        q025 = new PoolChunkList(this, q050, 25, 75);
        q000 = new PoolChunkList(this, q025, 1, 50);
        qInit = new PoolChunkList(this, q000, Integer.MIN_VALUE, 25);

        q100.prevList = q075;
        q075.prevList = q050;
        q050.prevList = q025;
        q025.prevList = q000;
        q000.prevList = null;
        qInit.prevList = qInit;
    }

    private PoolSubpage newSubpagePoolHead(int pageSize) {
        PoolSubpage head = new PoolSubpage(pageSize);
        head.prev = head;
        head.next = head;
        return head;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage[] newSubpagePoolArray(int size) {
        return new PoolSubpage[size];
    }

    PooledByteBuf allocate(PoolThreadCache cache, int reqCapacity, int maxCapacity) {
        PooledByteBuf buf = newByteBuf(maxCapacity);
        allocate(cache, buf, reqCapacity);
        return buf;
    }

    static int tinyIdx(int normCapacity) {
        return normCapacity >>> 4;
    }

    static int smallIdx(int normCapacity) {
        int tableIdx = 0;
        int i = normCapacity >>> 10;
        while (i != 0) {
            i >>>= 1;
            tableIdx++;
        }
        return tableIdx;
    }

    // capacity < pageSize
    boolean isTinyOrSmall(int normCapacity) {
        return (normCapacity & subpageOverflowMask) == 0;
    }

    // normCapacity < 512
    static boolean isTiny(int normCapacity) {
        return (normCapacity & 0xFFFFFE00) == 0;
    }

    private void allocate(PoolThreadCache cache, PooledByteBuf buf, final int reqCapacity) {
        final int normCapacity = normalizeCapacity(reqCapacity);
        if (isTinyOrSmall(normCapacity)) { // capacity < pageSize
            int tableIdx;
            PoolSubpage[] table;
            if (isTiny(normCapacity)) { // < 512
                if (cache.allocateTiny(this, buf, reqCapacity, normCapacity)) {
                    // was able to allocate out of the cache so move on
                    return;
                }
                tableIdx = tinyIdx(normCapacity);
                table = tinySubpagePools;
            } else {
                if (cache.allocateSmall(this, buf, reqCapacity, normCapacity)) {
                    // was able to allocate out of the cache so move on
                    return;
                }
                tableIdx = smallIdx(normCapacity);
                table = smallSubpagePools;
            }

            synchronized (this) {
                final PoolSubpage head = table[tableIdx];
                final PoolSubpage s = head.next;
                if (s != head) {
                    assert s.doNotDestroy && s.elemSize == normCapacity;
                    long handle = s.allocate();
                    assert handle >= 0;
                    s.chunk.initBufWithSubpage(buf, handle, reqCapacity);
                    return;
                }
            }
        } else if (normCapacity <= chunkSize) {
            if (cache.allocateNormal(this, buf, reqCapacity, normCapacity)) {
                // was able to allocate out of the cache so move on
                return;
            }
        } else {
            // Huge allocations are never served via the cache so just call allocateHuge
            allocateHuge(buf, reqCapacity);
            return;
        }
        allocateNormal(buf, reqCapacity, normCapacity);
    }

    private synchronized void allocateNormal(PooledByteBuf buf, int reqCapacity, int normCapacity) {
        if (q050.allocate(buf, reqCapacity, normCapacity) || q025.allocate(buf, reqCapacity, normCapacity) ||
                q000.allocate(buf, reqCapacity, normCapacity) || qInit.allocate(buf, reqCapacity, normCapacity) ||
                q075.allocate(buf, reqCapacity, normCapacity) || q100.allocate(buf, reqCapacity, normCapacity)) {
            return;
        }

        // Add a new chunk.
        PoolChunk c = newChunk(pageSize, maxOrder, pageShifts, chunkSize);
        long handle = c.allocate(normCapacity);
        assert handle > 0;
        c.initBuf(buf, handle, reqCapacity);
        qInit.add(c);
    }

    private void allocateHuge(PooledByteBuf buf, int reqCapacity) {
        buf.initUnpooled(newUnpooledChunk(reqCapacity), reqCapacity);
    }

    void free(PoolChunk chunk, long handle, int normCapacity, boolean sameThreads) {
        if (chunk.unpooled) {
            destroyChunk(chunk);
        } else {
            if (sameThreads) {
                PoolThreadCache cache = parent.threadCache.get();
                if (cache.add(this, chunk, handle, normCapacity)) {
                    // cached so not free it.
                    return;
                }
            }

            synchronized (this) {
                chunk.parent.free(chunk, handle);
            }
        }
    }

    PoolSubpage findSubpagePoolHead(int elemSize) {
        int tableIdx;
        PoolSubpage[] table;
        if (isTiny(elemSize)) { // < 512
            tableIdx = elemSize >>> 4;
            table = tinySubpagePools;
        } else {
            tableIdx = 0;
            elemSize >>>= 10;
            while (elemSize != 0) {
                elemSize >>>= 1;
                tableIdx++;
            }
            table = smallSubpagePools;
        }

        return table[tableIdx];
    }

    int normalizeCapacity(int reqCapacity) {
        if (reqCapacity < 0) {
            throw new IllegalArgumentException("capacity: " + reqCapacity + " (expected: 0+)");
        }
        if (reqCapacity >= chunkSize) {
            return reqCapacity;
        }

        if (!isTiny(reqCapacity)) { // >= 512
            // Doubled

            int normalizedCapacity = reqCapacity;
            normalizedCapacity--;
            normalizedCapacity |= normalizedCapacity >>> 1;
            normalizedCapacity |= normalizedCapacity >>> 2;
            normalizedCapacity |= normalizedCapacity >>> 4;
            normalizedCapacity |= normalizedCapacity >>> 8;
            normalizedCapacity |= normalizedCapacity >>> 16;
            normalizedCapacity++;

            if (normalizedCapacity < 0) {
                normalizedCapacity >>>= 1;
            }

            return normalizedCapacity;
        }

        // Quantum-spaced
        if ((reqCapacity & 15) == 0) {
            return reqCapacity;
        }

        return (reqCapacity & ~15) + 16;
    }

    void reallocate(PooledByteBuf buf, int newCapacity, boolean freeOldMemory) {
        if (newCapacity < 0 || newCapacity > buf.maxCapacity()) {
            throw new IllegalArgumentException("newCapacity: " + newCapacity);
        }

        int oldCapacity = buf.length;
        if (oldCapacity == newCapacity) {
            return;
        }

        PoolChunk oldChunk = buf.chunk;
        long oldHandle = buf.handle;
        Memory oldMemory = buf.memory;
        int oldOffset = buf.offset;
        int oldMaxLength = buf.maxLength;
        int readerIndex = buf.readerIndex();
        int writerIndex = buf.writerIndex();

        allocate(parent.threadCache.get(), buf, newCapacity);
        if (newCapacity > oldCapacity) {
            memoryCopy(oldMemory, oldOffset, buf.memory, buf.offset, oldCapacity);
        } else if (newCapacity < oldCapacity) {
            if (readerIndex < newCapacity) {
                if (writerIndex > newCapacity) {
                    writerIndex = newCapacity;
                }
                memoryCopy(oldMemory, oldOffset + readerIndex,
                        buf.memory, buf.offset + readerIndex, writerIndex - readerIndex);
            } else {
                readerIndex = writerIndex = newCapacity;
            }
        }

        buf.setIndex(readerIndex, writerIndex);

        if (freeOldMemory) {
            free(oldChunk, oldHandle, oldMaxLength, buf.initThread == Thread.currentThread());
        }
    }

    protected abstract PoolChunk newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize);

    protected abstract PoolChunk newUnpooledChunk(int capacity);

    protected abstract PooledByteBuf newByteBuf(int maxCapacity);

    protected abstract void memoryCopy(Memory src, int srcOffset, Memory dst,
                                       int dstOffset, int length);

    protected abstract void destroyChunk(PoolChunk chunk);

    public synchronized String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Chunk(s) at 0~25%:");
        buf.append("\n");
        buf.append(qInit);
        buf.append("\n");
        buf.append("Chunk(s) at 0~50%:");
        buf.append("\n");
        buf.append(q000);
        buf.append("\n");
        buf.append("Chunk(s) at 25~75%:");
        buf.append("\n");
        buf.append(q025);
        buf.append("\n");
        buf.append("Chunk(s) at 50~100%:");
        buf.append("\n");
        buf.append(q050);
        buf.append("\n");
        buf.append("Chunk(s) at 75~100%:");
        buf.append("\n");
        buf.append(q075);
        buf.append("\n");
        buf.append("Chunk(s) at 100%:");
        buf.append("\n");
        buf.append(q100);
        buf.append("\n");
        buf.append("tiny subpages:");
        for (int i = 1; i < tinySubpagePools.length; i++) {
            PoolSubpage head = tinySubpagePools[i];
            if (head.next == head) {
                continue;
            }

            buf.append("\n");
            buf.append(i);
            buf.append(": ");
            PoolSubpage s = head.next;
            for (; ; ) {
                buf.append(s);
                s = s.next;
                if (s == head) {
                    break;
                }
            }
        }
        buf.append("\n");
        buf.append("small subpages:");
        for (int i = 1; i < smallSubpagePools.length; i++) {
            PoolSubpage head = smallSubpagePools[i];
            if (head.next == head) {
                continue;
            }

            buf.append("\n");
            buf.append(i);
            buf.append(": ");
            PoolSubpage s = head.next;
            for (; ; ) {
                buf.append(s);
                s = s.next;
                if (s == head) {
                    break;
                }
            }
        }
        buf.append("\n");

        return buf.toString();
    }

    static final class DirectArena extends PoolArena {

        DirectArena(ByteBufAllocator parent, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
            super(parent, pageSize, maxOrder, pageShifts, chunkSize);
        }

        @Override
        protected PoolChunk newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize) {
            return new PoolChunk(this, UNSAFE.allocateMemory(chunkSize), pageSize, maxOrder,
                    pageShifts, chunkSize);
        }

        @Override
        protected PoolChunk newUnpooledChunk(int capacity) {
            return new PoolChunk(this, UNSAFE.allocateMemory(capacity), capacity);
        }

        @Override
        protected void destroyChunk(PoolChunk chunk) {
            UNSAFE.freeMemory(chunk.memory);
        }

        @Override
        protected PooledByteBuf newByteBuf(int maxCapacity) {
            return PooledByteBuf.newInstance(maxCapacity);
        }

        @Override
        protected void memoryCopy(Memory src, int srcOffset, Memory dst, int dstOffset, int length) {
            if (length == 0) {
                return;
            }

            UNSAFE.copyMemory(src.getAddress() + srcOffset, dst.getAddress() + dstOffset, length);
        }
    }
}
