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

import net.dongliu.direct.utils.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * allocator from netty.io
 */
public class PooledByteBufAllocator {

    private static final Logger logger = LoggerFactory.getLogger(PooledByteBufAllocator.class);

    private static final int ARENA_NUM;

    private static final int PAGE_SIZE;
    private static final int MAX_ORDER; // 8192 << 11 = 16 MiB per chunk

    //TODO: get the max direct memory setted.
    private static final int maxDirectMemory = Size.Mb(256);

    static {
        int defaultPageSize = 8192;
        Throwable pageSizeFallbackCause = null;
        try {
            validateAndCalculatePageShifts(defaultPageSize);
        } catch (Throwable t) {
            pageSizeFallbackCause = t;
            defaultPageSize = 8192;
        }
        PAGE_SIZE = defaultPageSize;

        int defaultMaxOrder = 11;
        Throwable maxOrderFallbackCause = null;
        try {
            validateAndCalculateChunkSize(PAGE_SIZE, defaultMaxOrder);
        } catch (Throwable t) {
            maxOrderFallbackCause = t;
            defaultMaxOrder = 11;
        }
        MAX_ORDER = defaultMaxOrder;

        // Determine reasonable default for nDirectArena.
        // Assuming each arena has 3 chunks, the pool should not consume more than 50% of max memory.
        final Runtime runtime = Runtime.getRuntime();
        final int defaultChunkSize = PAGE_SIZE << MAX_ORDER;
        ARENA_NUM = Math.max(0,
                Math.min(runtime.availableProcessors(), maxDirectMemory / defaultChunkSize / 2 / 3));
    }

    public static final PooledByteBufAllocator DEFAULT = new PooledByteBufAllocator();

    private final PoolArena[] arenas;

    final ThreadLocal<PoolThreadCache> threadCache = new ThreadLocal<PoolThreadCache>() {
        private final AtomicInteger index = new AtomicInteger();

        @Override
        protected PoolThreadCache initialValue() {
            final int idx = index.getAndIncrement();
            final PoolArena arena = arenas[Math.abs(idx % arenas.length)];

            return new PoolThreadCache(arena);
        }
    };

    public PooledByteBufAllocator() {
        this(ARENA_NUM, PAGE_SIZE, MAX_ORDER);
    }

    public PooledByteBufAllocator(int nArena, int pageSize, int maxOrder) {
        final int chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

        if (nArena <= 0) {
            throw new IllegalArgumentException("nDirectArea: " + nArena + " (expected: >= 0)");
        }

        int pageShifts = validateAndCalculatePageShifts(pageSize);

        arenas = new PoolArena[nArena];
        for (int i = 0; i < arenas.length; i++) {
            arenas[i] = new PoolArena(this, pageSize, maxOrder, pageShifts, chunkSize);
        }
    }

    private static int validateAndCalculatePageShifts(int pageSize) {
        int minPageSize = 4096;
        if (pageSize < minPageSize) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: 4096+)");
        }

        // Ensure pageSize is power of 2.
        boolean found1 = false;
        int pageShifts = 0;
        for (int i = pageSize; i != 0; i >>= 1) {
            if ((i & 1) != 0) {
                if (!found1) {
                    found1 = true;
                } else {
                    throw new IllegalArgumentException("pageSize: " + pageSize +
                            " (expected: power of 2");
                }
            } else {
                if (!found1) {
                    pageShifts++;
                }
            }
        }
        return pageShifts;
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        int maxChunkSize = (int) (((long) Integer.MAX_VALUE + 1) / 2);
        for (int i = maxOrder; i > 0; i--) {
            if (chunkSize > maxChunkSize / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d",
                        pageSize, maxOrder, maxChunkSize));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }


    public PooledByteBuf allocate(int initialCapacity) {
        PoolThreadCache cache = threadCache.get();
        PoolArena arena = cache.arena;

        return arena.allocate(cache, initialCapacity);
    }

}