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

package net.dongliu.direct.memory.je;

import net.dongliu.direct.memory.UnsafeMemory;

class PoolArena {

    final PooledByteBufAllocator parent;

    private final int pageSize;
    private final int maxOrder;
    private final int pageShifts;
    private final int chunkSize;
    private final int subpageOverflowMask;

    private final PoolSubpage[] tinySubpagePools;
    private final PoolSubpage[] smallSubpagePools;

    private final PoolChunkList q050;
    private final PoolChunkList q025;
    private final PoolChunkList q000;
    private final PoolChunkList qInit;
    private final PoolChunkList q075;
    private final PoolChunkList q100;

    protected PoolArena(PooledByteBufAllocator parent, int pageSize, int maxOrder, int pageShifts,
                        int chunkSize) {
        this.parent = parent;
        this.pageSize = pageSize;
        this.maxOrder = maxOrder;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        subpageOverflowMask = ~(pageSize - 1);

        tinySubpagePools = newSubpagePoolArray(512 >>> 4);
        for (int i = 0; i < tinySubpagePools.length; i++) {
            tinySubpagePools[i] = newSubpagePoolHead(pageSize);
        }

        smallSubpagePools = newSubpagePoolArray(pageShifts - 9);
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

    PooledByteBuf allocate(PoolThreadCache cache, int reqCapacity) {
        PooledByteBuf buf = PooledByteBuf.newInstance();
        allocate(cache, buf, reqCapacity);
        return buf;
    }

    private void allocate(PoolThreadCache cache, PooledByteBuf buf, final int reqCapacity) {
        final int normCapacity = normalizeCapacity(reqCapacity);
        if ((normCapacity & subpageOverflowMask) == 0) { // capacity < pageSize
            int tableIdx;
            PoolSubpage[] table;
            if ((normCapacity & 0xFFFFFE00) == 0) { // < 512
                tableIdx = normCapacity >>> 4;
                table = tinySubpagePools;
            } else {
                tableIdx = 0;
                int i = normCapacity >>> 10;
                while (i != 0) {
                    i >>>= 1;
                    tableIdx++;
                }
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
        } else if (normCapacity > chunkSize) {
            return;
        }

        allocateNormal(buf, reqCapacity, normCapacity);
    }

    private synchronized void allocateNormal(PooledByteBuf buf, int reqCapacity, int normCapacity) {
        if (q050.allocate(buf, reqCapacity, normCapacity)
                || q025.allocate(buf, reqCapacity, normCapacity)
                || q000.allocate(buf, reqCapacity, normCapacity)
                || qInit.allocate(buf, reqCapacity, normCapacity)
                || q075.allocate(buf, reqCapacity, normCapacity)
                || q100.allocate(buf, reqCapacity, normCapacity)) {
            return;
        }

        // Add a new chunk.
        PoolChunk poolChunk = newChunk(pageSize, maxOrder, pageShifts, chunkSize);
        long handle = poolChunk.allocate(normCapacity);
        assert handle > 0;
        poolChunk.initBuf(buf, handle, reqCapacity);
        qInit.add(poolChunk);
    }

    void free(PoolChunk chunk, long handle) {
        synchronized (this) {
            chunk.parent.free(chunk, handle);
        }
    }

    PoolSubpage findSubpagePoolHead(int elemSize) {
        int tableIdx;
        PoolSubpage[] table;
        if ((elemSize & 0xFFFFFE00) == 0) { // < 512
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

    private int normalizeCapacity(int reqCapacity) {
        if (reqCapacity < 0) {
            throw new IllegalArgumentException("capacity: " + reqCapacity + " (expected: 0+)");
        }
        if (reqCapacity >= chunkSize) {
            return reqCapacity;
        }

        if ((reqCapacity & 0xFFFFFE00) != 0) { // >= 512
            // Doubled

            int normalizedCapacity = reqCapacity;
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

    protected PoolChunk newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        return new PoolChunk(
                this, UnsafeMemory.allocate(chunkSize), pageSize, maxOrder, pageShifts, chunkSize);
    }

    protected void destroyChunk(PoolChunk chunk) {
        chunk.memory.dispose();
    }

}
