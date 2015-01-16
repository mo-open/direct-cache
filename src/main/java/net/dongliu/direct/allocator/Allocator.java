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

import net.dongliu.direct.utils.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * byte buffer allocator
 */
public class Allocator {

    private static final Logger logger = LoggerFactory.getLogger(Allocator.class);

    private final ByteBuf emptyBuf;
    private static final int DEFAULT_NUM_DIRECT_ARENA;
    private static final int DEFAULT_PAGE_SIZE;
    private static final int DEFAULT_MAX_ORDER; // 8192 << 11 = 16 MiB per chunk
    private static final int DEFAULT_TINY_CACHE_SIZE;
    private static final int DEFAULT_SMALL_CACHE_SIZE;
    private static final int DEFAULT_NORMAL_CACHE_SIZE;
    private static final int DEFAULT_MAX_CACHED_BUFFER_CAPACITY;
    private static final int DEFAULT_CACHE_TRIM_INTERVAL;

    private static final int MIN_PAGE_SIZE = 4096;
    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

    // current total memory allocated
    private final AtomicLong used = new AtomicLong(0);
    // max memory this allocator can allocate
    private final long capacity;

    static {
        int defaultPageSize = 8192;
        try {
            validateAndCalculatePageShifts(defaultPageSize);
        } catch (Throwable t) {
            defaultPageSize = 8192;
        }
        DEFAULT_PAGE_SIZE = defaultPageSize;

        int defaultMaxOrder = 11;
        try {
            validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
        } catch (Throwable t) {
            defaultMaxOrder = 11;
        }
        DEFAULT_MAX_ORDER = defaultMaxOrder;

        // Assuming each arena has 3 chunks, the pool should not consume more than 50% of max memory.
        final Runtime runtime = Runtime.getRuntime();
        final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
        DEFAULT_NUM_DIRECT_ARENA = runtime.availableProcessors();

        // cache sizes
        DEFAULT_TINY_CACHE_SIZE = 512;
        DEFAULT_SMALL_CACHE_SIZE = 256;
        DEFAULT_NORMAL_CACHE_SIZE = 64;

        // 32 kb is the default maximum size of the cached buffer. Similar to what is explained in
        // 'Scalable memory allocation using jemalloc'
        DEFAULT_MAX_CACHED_BUFFER_CAPACITY = Size.Kb(32);

        // the number of threshold of allocations when cached entries will be freed up if not frequently used
        DEFAULT_CACHE_TRIM_INTERVAL = 8192;
    }

    private final PoolArena[] directArenas;
    private final int tinyCacheSize;
    private final int smallCacheSize;
    private final int normalCacheSize;

    final PoolThreadLocalCache threadCache;


    public Allocator(long capacity) {
        this(DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER, capacity);
    }

    public Allocator(int nDirectArena, int pageSize, int maxOrder, long capacity) {
        this(nDirectArena, pageSize, maxOrder,
                capacity, DEFAULT_TINY_CACHE_SIZE, DEFAULT_SMALL_CACHE_SIZE, DEFAULT_NORMAL_CACHE_SIZE);
    }

    public Allocator(int nDirectArena, int pageSize, int maxOrder,
                     long capacity, int tinyCacheSize, int smallCacheSize, int normalCacheSize) {
        this.capacity = capacity;
        emptyBuf = new EmptyByteBuf(this);
        threadCache = new PoolThreadLocalCache();
        this.tinyCacheSize = tinyCacheSize;
        this.smallCacheSize = smallCacheSize;
        this.normalCacheSize = normalCacheSize;
        final int chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

        if (nDirectArena <= 0) {
            throw new IllegalArgumentException("nDirectArea: " + nDirectArena + " (expected: >= 0)");
        }

        int pageShifts = validateAndCalculatePageShifts(pageSize);

        directArenas = newArenaArray(nDirectArena);
        for (int i = 0; i < directArenas.length; i++) {
            directArenas[i] = new PoolArena.DirectArena(this, pageSize, maxOrder, pageShifts, chunkSize);
        }
    }

    private static PoolArena[] newArenaArray(int size) {
        return new PoolArena[size];
    }

    private static int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + "+)");
        }

        if ((pageSize & pageSize - 1) != 0) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
        }

        // Logarithm base 2. At this point we know that pageSize is a power of two.
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i--) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    /**
     * allocate buf
     *
     * @return null if exceed max size
     */
    public ByteBuf directBuffer(int capacity) {
        if (capacity == 0) {
            return emptyBuf;
        }
        return newDirectBuffer(capacity);
    }

    private ByteBuf newDirectBuffer(int capacity) {
        if (used.get() > this.capacity) {
            return null;
        }
        PoolThreadCache cache = threadCache.get();
        PoolArena directArena = cache.directArena;
        UnsafeByteBuf buf = directArena.allocate(cache, capacity);
        used.getAndAdd(buf.capacity());
        return buf;
    }

    /**
     * max memory can allocate
     */
    public long getCapacity() {
        return capacity;
    }

    /**
     * used memory
     */
    public AtomicLong getUsed() {
        return used;
    }

    final class PoolThreadLocalCache extends ThreadLocal<PoolThreadCache> {
        private final AtomicInteger index = new AtomicInteger();

        @Override
        protected PoolThreadCache initialValue() {
            final int idx = index.getAndIncrement();
            final PoolArena directArena;

            directArena = directArenas[Math.abs(idx % directArenas.length)];

            return new PoolThreadCache(
                    directArena, tinyCacheSize, smallCacheSize, normalCacheSize,
                    DEFAULT_MAX_CACHED_BUFFER_CAPACITY, DEFAULT_CACHE_TRIM_INTERVAL);
        }
    }
}
