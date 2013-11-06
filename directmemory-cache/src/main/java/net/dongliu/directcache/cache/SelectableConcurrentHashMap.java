package net.dongliu.directcache.cache;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.struct.ValueWrapper;

/**
 * SelectableConcurrentHashMap subclasses a repackaged version of ConcurrentHashMap
 * ito allow efficient random sampling of the map values.
 * <p/>
 * The random sampling technique involves randomly selecting a map Segment, and then
 * selecting a number of random entry chains from that segment.
 */
public class SelectableConcurrentHashMap {

    /**
     * The maximum capacity, used if a higher value is implicitly
     * specified by either of the constructors with arguments.  MUST
     * be a power of two <= 1<<30 to ensure that entries are indexable
     * using ints.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The maximum number of segments to allow; used to bound
     * constructor arguments.
     */
    private static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

    /**
     * Number of unsynchronized retries in size and containsValue
     * methods before resorting to locking. This is used to avoid
     * unbounded retries if tables undergo continuous modification
     * which would make it impossible to obtain an accurate result.
     */
    private static final int RETRIES_BEFORE_LOCK = 2;

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    private final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    private final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table
     */
    private final Segment[] segments;

    private final Random rndm = new Random();
    private volatile long maxSize;
    private final CacheEventListener cacheEventListener;

    private Set<Object> keySet;
    private Set<Map.Entry<Object, ValueWrapper>> entrySet;
    private Collection<ValueWrapper> values;

    private final Allocator allocator;

    public SelectableConcurrentHashMap(Allocator allocator, int initialCapacity,
                                       float loadFactor, int concurrency, final long maximumSize,
                                       final CacheEventListener cacheEventListener) {
        if (!(loadFactor > 0) || initialCapacity < 0 || concurrency <= 0)
            throw new IllegalArgumentException();

        this.allocator = allocator;

        if (concurrency > MAX_SEGMENTS)
            concurrency = MAX_SEGMENTS;

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrency) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        this.segments = new Segment[ssize];

        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        int c = initialCapacity / ssize;
        if (c * ssize < initialCapacity)
            ++c;
        int cap = 1;
        while (cap < c)
            cap <<= 1;

        for (int i = 0; i < this.segments.length; ++i)
            this.segments[i] = createSegment(cap, loadFactor);

        this.maxSize = maximumSize;
        this.cacheEventListener = cacheEventListener;
    }

    public void setMaxSize(final long maxSize) {
        this.maxSize = maxSize;
    }

    public ValueWrapper[] getRandomValues(final int size, Object keyHint) {
        ArrayList<ValueWrapper> sampled = new ArrayList<ValueWrapper>(size * 2);

        // pick a random starting point in the map
        int randomHash = rndm.nextInt();

        final int segmentStart;
        if (keyHint == null) {
            segmentStart = (randomHash >>> segmentShift) & segmentMask;
        } else {
            segmentStart = (hash(keyHint.hashCode()) >>> segmentShift) & segmentMask;
        }

        int segmentIndex = segmentStart;
        do {
            final HashEntry[] table = segments[segmentIndex].table;
            final int tableStart = randomHash & (table.length - 1);
            int tableIndex = tableStart;
            do {
                for (HashEntry e = table[tableIndex]; e != null; e = e.next) {
                    ValueWrapper value = e.value;
                    if (value != null) {
                        sampled.add(value);
                    }
                }

                if (sampled.size() >= size) {
                    return sampled.toArray(new ValueWrapper[sampled.size()]);
                }

                //move to next table slot
                tableIndex = (tableIndex + 1) & (table.length - 1);
            } while (tableIndex != tableStart);

            //move to next segment
            segmentIndex = (segmentIndex + 1) & segmentMask;
        } while (segmentIndex != segmentStart);

        return sampled.toArray(new ValueWrapper[sampled.size()]);
    }

    /**
     * Return an object of the kind which will be stored when
     * the element is going to be inserted
     *
     * @param e the element
     * @return an object looking-alike the stored one
     */
    public Object storedObject(ValueWrapper e) {
        return new HashEntry(null, 0, null, e, 0);
    }

    /**
     * Returns the number of key-value mappings in this map without locking anything.
     * This may not give the exact element count as locking is avoided.
     * If the map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-value mappings in this map
     */
    public int quickSize() {
        final Segment[] segments = this.segments;
        long sum = 0;
        for (Segment seg : segments) {
            sum += seg.count;
        }

        if (sum > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) sum;
        }
    }

    public boolean isEmpty() {
        final Segment[] segments = this.segments;
        /*
         * We keep track of per-segment modCounts to avoid ABA
         * problems in which an element in one segment was added and
         * in another removed during traversal, in which case the
         * table was never actually empty at any point. Note the
         * similar use of modCounts in the size() and containsValue()
         * methods, which are the only other methods also susceptible
         * to ABA problems.
         */
        int[] mc = new int[segments.length];
        int mcsum = 0;
        for (int i = 0; i < segments.length; ++i) {
            if (segments[i].count != 0)
                return false;
            else
                mcsum += mc[i] = segments[i].modCount;
        }
        // If mcsum happens to be zero, then we know we got a snapshot
        // before any modifications at all were made.  This is
        // probably common enough to bother tracking.
        if (mcsum != 0) {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].count != 0 ||
                        mc[i] != segments[i].modCount)
                    return false;
            }
        }
        return true;
    }

    public int size() {
        final Segment[] segments = this.segments;

        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            int[] mc = new int[segments.length];
            long check = 0;
            long sum = 0;
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                sum += segments[i].count;
                mcsum += mc[i] = segments[i].modCount;
            }
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    check += segments[i].count;
                    if (mc[i] != segments[i].modCount) {
                        check = -1; // force retry
                        break;
                    }
                }
            }
            if (check == sum) {
                if (sum > Integer.MAX_VALUE) {
                    return Integer.MAX_VALUE;
                } else {
                    return (int) sum;
                }
            }
        }

        long sum = 0;
        for (Segment segment : segments) {
            segment.readLock().lock();
        }
        try {
            for (Segment segment : segments) {
                sum += segment.count;
            }
        } finally {
            for (Segment segment : segments) {
                segment.readLock().unlock();
            }
        }

        if (sum > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) sum;
        }
    }

    public ReentrantReadWriteLock lockFor(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash);
    }

    public ReentrantReadWriteLock[] locks() {
        return segments;
    }

    public ValueWrapper get(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).get(key, hash);
    }

    public boolean containsKey(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).containsKey(key, hash);
    }

    public boolean containsValue(Object value) {
        if (value == null)
            throw new NullPointerException();

        // See explanation of modCount use above

        final Segment[] segments = this.segments;
        int[] mc = new int[segments.length];

        // Try a few times without locking
        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            int sum = 0;
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                int c = segments[i].count;
                mcsum += mc[i] = segments[i].modCount;
                if (segments[i].containsValue(value))
                    return true;
            }
            boolean cleanSweep = true;
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    int c = segments[i].count;
                    if (mc[i] != segments[i].modCount) {
                        cleanSweep = false;
                        break;
                    }
                }
            }
            if (cleanSweep)
                return false;
        }

        // Resort to locking all segments
        for (int i = 0; i < segments.length; ++i)
            segments[i].readLock().lock();
        try {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].containsValue(value)) {
                    return true;
                }
            }
        } finally {
            for (int i = 0; i < segments.length; ++i)
                segments[i].readLock().unlock();
        }
        return false;
    }

    /**
     * set key - element. if has old value, also returnTo the old pointer.
     *
     * @param key
     * @param element
     * @param sizeOf
     * @return the old ValueWrapper, null if not exists
     */
    public ValueWrapper put(Object key, ValueWrapper element, long sizeOf) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, element, sizeOf, false, true);
    }

    /**
     * set key - element if absent.
     *
     * @param key
     * @param element
     * @param sizeOf
     * @return
     */
    public ValueWrapper putIfAbsent(Object key, ValueWrapper element, long sizeOf) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, element, sizeOf, true, true);
    }

    /**
     * remove also returnTo Poniter.
     *
     * @param key
     * @return
     */
    public ValueWrapper remove(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).remove(key, hash, null);
    }

    public boolean remove(Object key, Object value) {
        int hash = hash(key.hashCode());
        if (value == null)
            return false;
        return segmentFor(hash).remove(key, hash, value) != null;
    }

    /**
     * clear also cause all ValueWrapper to be returnTo.
     */
    public void clear() {
        for (int i = 0; i < segments.length; ++i)
            segments[i].clear();
    }

    public Set<Object> keySet() {
        Set<Object> ks = keySet;
        return (ks != null) ? ks : (keySet = new KeySet());
    }

    public Collection<ValueWrapper> values() {
        Collection<ValueWrapper> vs = values;
        return (vs != null) ? vs : (values = new Values());
    }

    public Set<Entry<Object, ValueWrapper>> entrySet() {
        Set<Entry<Object, ValueWrapper>> es = entrySet;
        return (es != null) ? es : (entrySet = new EntrySet());
    }

    protected Segment createSegment(int initialCapacity, float lf) {
        return new Segment(initialCapacity, lf);
    }

    public boolean evict() {
        return getRandomSegment().evict();
    }

    private Segment getRandomSegment() {
        int randomHash = rndm.nextInt();
        return segments[((randomHash >>> segmentShift) & segmentMask)];
    }

    public void recalculateSize(Object key) {
        int hash = hash(key.hashCode());
        segmentFor(hash).recalculateSize(key, hash);
    }

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash the hash code for the key
     * @return the segment
     */
    protected final Segment segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    protected final List<Segment> segments() {
        return Collections.unmodifiableList(Arrays.asList(segments));
    }

    public class Segment extends ReentrantReadWriteLock {

        private static final int MAX_EVICTION = 5;

        /**
         * The number of elements in this segment's region.
         */
        protected volatile int count;

        /**
         * Number of updates that alter the size of the table. This is
         * used during bulk-read methods to make sure they see a
         * consistent snapshot: If modCounts change during a traversal
         * of segments computing size or checking containsValue, then
         * we might have an inconsistent view of state so (usually)
         * must retry.
         */
        int modCount;

        /**
         * The table is rehashed when its size exceeds this threshold.
         * (The value of this field is always <tt>(int)(capacity *
         * loadFactor)</tt>.)
         */
        int threshold;

        /**
         * The per-segment table.
         */
        protected volatile HashEntry[] table;

        /**
         * The load factor for the hash table.  Even though this value
         * is same for all segments, it is replicated to avoid needing
         * links to outer object.
         *
         * @serial
         */
        final float loadFactor;

        private Iterator<HashEntry> evictionIterator = iterator();

        protected Segment(int initialCapacity, float lf) {
            loadFactor = lf;
            setTable(new HashEntry[initialCapacity]);
        }

        protected void preRemove(HashEntry e) {
            e.value.returnTo(allocator);
        }

        protected void postInstall(Object key, ValueWrapper value) {

        }

        /**
         * Sets table to new HashEntry array.
         * Call only while holding lock or in constructor.
         */
        void setTable(HashEntry[] newTable) {
            threshold = (int) (newTable.length * loadFactor);
            table = newTable;
        }

        /**
         * Returns properly casted first entry of bin for given hash.
         */
        protected HashEntry getFirst(int hash) {
            HashEntry[] tab = table;
            return tab[hash & (tab.length - 1)];
        }

        protected HashEntry createHashEntry(Object key, int hash, HashEntry next,
                                            ValueWrapper value, long sizeOf) {
            return new HashEntry(key, hash, next, value, sizeOf);
        }

        protected HashEntry relinkHashEntry(HashEntry e, HashEntry next) {
            return new HashEntry(e.key, e.hash, next, e.value, e.sizeOf);
        }

        protected void clear() {
            writeLock().lock();
            try {
                if (count != 0) {
                    HashEntry[] tab = table;
                    for (int i = 0; i < tab.length; i++) {
                        tab[i].value.returnTo(allocator);
                        tab[i] = null;
                    }
                    ++modCount;
                    count = 0; // write-volatile
                }
            } finally {
                writeLock().unlock();
            }
        }

        ValueWrapper remove(Object key, int hash, Object value) {
            writeLock().lock();
            try {
                int c = count - 1;
                HashEntry[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry first = tab[index];
                HashEntry e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key)))
                    e = e.next;

                ValueWrapper oldValue = null;
                if (e != null) {
                    ValueWrapper v = e.value;
                    if (value == null || value.equals(v)) {
                        oldValue = v;
                        ++modCount;
                        preRemove(e);
                        // All entries following removed node can stay
                        // in list, but all preceding ones need to be
                        // cloned.
                        HashEntry newFirst = e.next;
                        for (HashEntry p = first; p != e; p = p.next)
                            newFirst = relinkHashEntry(p, newFirst);
                        tab[index] = newFirst;
                        count = c; // write-volatile
//                        poolAccessor.delete(e.sizeOf);
                    }
                }
                return oldValue;
            } finally {
                writeLock().unlock();
            }
        }

        public void recalculateSize(Object key, int hash) {
            ValueWrapper value = null;
            long oldSize = 0;
            readLock().lock();
            try {
                HashEntry[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry first = tab[index];
                HashEntry e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key))) {
                    e = e.next;
                }
                if (e != null) {
                    key = e.key;
                    value = e.value;
                    oldSize = e.sizeOf;
                }
            } finally {
                readLock().unlock();
            }
            if (value != null) {
//                long delta = poolAccessor.replace(oldSize, key, value, storedObject(value), true);
                writeLock().lock();
                try {
                    HashEntry e = getFirst(hash);
                    while (e != null && key != e.key) {
                        e = e.next;
                    }

//                    if (e != null && e.value == value && oldSize == e.sizeOf) {
//                        e.sizeOf = oldSize + delta;
//                    } else {
//                        poolAccessor.delete(delta);
//                    }
                } finally {
                    writeLock().unlock();
                }
            }
        }

        protected ValueWrapper put(Object key, int hash, ValueWrapper value, long sizeOf,
                              boolean onlyIfAbsent, boolean fire) {
            ValueWrapper[] evicted = new ValueWrapper[MAX_EVICTION];
            writeLock().lock();
            try {
                int c = count;
                if (c++ > threshold) // ensure capacity
                    rehash();
                HashEntry[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry first = tab[index];
                HashEntry e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key)))
                    e = e.next;

                ValueWrapper oldValue;
                if (e != null) {
                    oldValue = e.value;
                    if (!onlyIfAbsent) {
                        e.value = value;
                        e.sizeOf = sizeOf;
                        if (fire) {
                            postInstall(key, value);
                        }
                    }
                } else {
                    oldValue = null;
                    ++modCount;
                    tab[index] = createHashEntry(key, hash, first, value, sizeOf);
                    count = c; // write-volatile
                    if (fire) {
                        postInstall(key, value);
                    }
                }

                if (onlyIfAbsent && oldValue != null || !onlyIfAbsent) {
                    if (SelectableConcurrentHashMap.this.maxSize > 0) {
                        int runs = Math.min(MAX_EVICTION, SelectableConcurrentHashMap.this.quickSize()
                                - (int) SelectableConcurrentHashMap.this.maxSize);
                        while (runs-- > 0) {
                            ValueWrapper evict = nextExpiredOrToEvict(value);
                            if (evict != null) {
                                ValueWrapper removed;
                                while ((removed = remove(evict.getKey(), hash(evict.getKey().hashCode()), null))
                                        == null) {
                                    evict = nextExpiredOrToEvict(value);
                                    if (evict == null) {
                                        break;
                                    }
                                }
                                evicted[runs] = removed;
                            }
                        }
                    }
                }
                return oldValue;
            } finally {
                writeLock().unlock();
                for (ValueWrapper element : evicted) {
                    notifyEvictionOrExpiry(element);
                }
            }
        }

        private void notifyEvictionOrExpiry(final ValueWrapper element) {
            if (element != null && cacheEventListener != null) {
                if (element.isExpired()) {
                    cacheEventListener.notiryExpired(element, false);
                } else {
                    cacheEventListener.notifyEvicted(element, false);
                }
            }
        }

        ValueWrapper get(final Object key, final int hash) {
            readLock().lock();
            try {
                if (count != 0) { // read-volatile
                    HashEntry e = getFirst(hash);
                    while (e != null) {
                        if (e.hash == hash && key.equals(e.key)) {
                            e.accessed = true;
                            return e.value;
                        }
                        e = e.next;
                    }
                }
                return null;
            } finally {
                readLock().unlock();
            }
        }

        boolean containsKey(final Object key, final int hash) {
            readLock().lock();
            try {
                if (count != 0) { // read-volatile
                    HashEntry e = getFirst(hash);
                    while (e != null) {
                        if (e.hash == hash && key.equals(e.key))
                            return true;
                        e = e.next;
                    }
                }
                return false;
            } finally {
                readLock().unlock();
            }
        }

        boolean containsValue(Object value) {
            readLock().lock();
            try {
                if (count != 0) { // read-volatile
                    HashEntry[] tab = table;
                    int len = tab.length;
                    for (int i = 0; i < len; i++) {
                        for (HashEntry e = tab[i]; e != null; e = e.next) {
                            ValueWrapper v = e.value;
                            if (value.equals(v))
                                return true;
                        }
                    }
                }
                return false;
            } finally {
                readLock().unlock();
            }
        }

        private ValueWrapper nextExpiredOrToEvict(final ValueWrapper justAdded) {

            ValueWrapper lastValueWrapper = null;
            int i = 0;

            while (i++ < count) {
                if (!evictionIterator.hasNext()) {
                    evictionIterator = iterator();
                }
                final HashEntry next = evictionIterator.next();
                if (next.value.isExpired() || !next.accessed) {
                    return next.value;
                } else {
                    if (next.value != justAdded) {
                        lastValueWrapper = next.value;
                    }
                }
            }

            return lastValueWrapper;
        }

        protected Iterator<HashEntry> iterator() {
            return new SegmentIterator(this);
        }

        private boolean evict() {
            ValueWrapper remove = null;
            writeLock().lock();
            try {
                ValueWrapper evict = nextExpiredOrToEvict(null);
                if (evict != null) {
                    remove = remove(evict.getKey(), hash(evict.getKey().hashCode()), null);
                }
            } finally {
                writeLock().unlock();
            }
            notifyEvictionOrExpiry(remove);
            return remove != null;
        }

        void rehash() {
            HashEntry[] oldTable = table;
            int oldCapacity = oldTable.length;
            if (oldCapacity >= MAXIMUM_CAPACITY)
                return;

            /*
             * Reclassify nodes in each list to new Map.  Because we are
             * using power-of-two expansion, the elements from each bin
             * must either stay at same index, or move with a power of two
             * offset. We eliminate unnecessary node creation by catching
             * cases where old nodes can be reused because their next
             * fields won't change. Statistically, at the default
             * threshold, only about one-sixth of them need cloning when
             * a table doubles. The nodes they replace will be garbage
             * collectable as soon as they are no longer referenced by any
             * reader thread that may be in the midst of traversing table
             * right now.
             */

            HashEntry[] newTable = new HashEntry[oldCapacity << 1];
            threshold = (int) (newTable.length * loadFactor);
            int sizeMask = newTable.length - 1;
            for (int i = 0; i < oldCapacity; i++) {
                // We need to guarantee that any existing reads of old Map can
                //  proceed. So we cannot yet null out each bin.
                HashEntry e = oldTable[i];

                if (e != null) {
                    HashEntry next = e.next;
                    int idx = e.hash & sizeMask;

                    //  Single node on list
                    if (next == null)
                        newTable[idx] = e;

                    else {
                        // Reuse trailing consecutive sequence at same slot
                        HashEntry lastRun = e;
                        int lastIdx = idx;
                        for (HashEntry last = next;
                             last != null;
                             last = last.next) {
                            int k = last.hash & sizeMask;
                            if (k != lastIdx) {
                                lastIdx = k;
                                lastRun = last;
                            }
                        }
                        newTable[lastIdx] = lastRun;

                        // Clone all remaining nodes
                        for (HashEntry p = e; p != lastRun; p = p.next) {
                            int k = p.hash & sizeMask;
                            HashEntry n = newTable[k];
                            newTable[k] = relinkHashEntry(p, n);
                        }
                    }
                }
            }
            table = newTable;
        }
    }

    public static class HashEntry {
        public final Object key;
        public final int hash;
        public final HashEntry next;

        public volatile ValueWrapper value;

        public volatile long sizeOf;
        public volatile boolean accessed = true;

        protected HashEntry(Object key, int hash, HashEntry next, ValueWrapper value, long sizeOf) {
            this.key = key;
            this.hash = hash;
            this.next = next;
            this.value = value;
            this.sizeOf = sizeOf;
        }
    }

    private class SegmentIterator implements Iterator<HashEntry> {

        int nextTableIndex;
        HashEntry[] currentTable;
        HashEntry nextEntry;
        HashEntry lastReturned;
        private final Segment seg;

        private SegmentIterator(final Segment memoryStoreSegment) {
            nextTableIndex = -1;
            this.seg = memoryStoreSegment;
            advance();
        }

        public boolean hasNext() {
            return nextEntry != null;
        }

        public HashEntry next() {
            if (nextEntry == null)
                return null;
            lastReturned = nextEntry;
            advance();
            return lastReturned;
        }

        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }

        final void advance() {
            if (nextEntry != null && (nextEntry = nextEntry.next) != null)
                return;
            while (nextTableIndex >= 0) {
                if ((nextEntry = currentTable[nextTableIndex--]) != null)
                    return;
            }
            if (seg.count != 0) {
                currentTable = seg.table;
                for (int j = currentTable.length - 1; j >= 0; --j) {
                    if ((nextEntry = currentTable[j]) != null) {
                        nextTableIndex = j - 1;
                        return;
                    }
                }
            }
        }
    }


    final class KeySet extends AbstractSet<Object> {

        @Override
        public Iterator<Object> iterator() {
            return new KeyIterator();
        }

        @Override
        public int size() {
            return SelectableConcurrentHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return SelectableConcurrentHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return SelectableConcurrentHashMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            return SelectableConcurrentHashMap.this.remove(o) != null;
        }

        @Override
        public void clear() {
            SelectableConcurrentHashMap.this.clear();
        }

        @Override
        public Object[] toArray() {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this)
                c.add(object);
            return c.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this)
                c.add(object);
            return c.toArray(a);
        }
    }

    final class Values extends AbstractCollection<ValueWrapper> {

        @Override
        public Iterator<ValueWrapper> iterator() {
            return new ValueIterator();
        }

        @Override
        public int size() {
            return SelectableConcurrentHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return SelectableConcurrentHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return SelectableConcurrentHashMap.this.containsValue(o);
        }

        @Override
        public void clear() {
            SelectableConcurrentHashMap.this.clear();
        }

        @Override
        public Object[] toArray() {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this)
                c.add(object);
            return c.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this)
                c.add(object);
            return c.toArray(a);
        }
    }

    final class EntrySet extends AbstractSet<Entry<Object, ValueWrapper>> {

        @Override
        public Iterator<Entry<Object, ValueWrapper>> iterator() {
            return new EntryIterator();
        }

        @Override
        public int size() {
            return SelectableConcurrentHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return SelectableConcurrentHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry<?, ?> e = (Entry<?, ?>) o;
            ValueWrapper v = SelectableConcurrentHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry<?, ?> e = (Entry<?, ?>) o;
            return SelectableConcurrentHashMap.this.remove(e.getKey(), e.getValue());
        }

        @Override
        public void clear() {
            SelectableConcurrentHashMap.this.clear();
        }

        @Override
        public Object[] toArray() {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this)
                c.add(object);
            return c.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this)
                c.add(object);
            return c.toArray(a);
        }
    }

    class KeyIterator extends HashEntryIterator implements Iterator<Object> {

        @Override
        public Object next() {
            return nextEntry().key;
        }
    }

    final class ValueIterator extends HashEntryIterator implements Iterator<ValueWrapper> {

        @Override
        public ValueWrapper next() {
            return nextEntry().value;
        }
    }

    final class EntryIterator extends HashEntryIterator implements Iterator<Entry<Object, ValueWrapper>> {

        @Override
        public Entry<Object, ValueWrapper> next() {
            HashEntry entry = nextEntry();
            final Object key = entry.key;
            final ValueWrapper value = entry.value;
            return new Entry<Object, ValueWrapper>() {

                public Object getKey() {
                    return key;
                }

                public ValueWrapper getValue() {
                    return value;
                }

                public ValueWrapper setValue(ValueWrapper value) {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    abstract class HashEntryIterator extends HashIterator {
        private HashEntry myNextEntry;

        public HashEntryIterator() {
            myNextEntry = advanceToNextEntry();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }

        @Override
        public HashEntry nextEntry() {
            if (myNextEntry == null) {
                throw new NoSuchElementException();
            }
            HashEntry entry = myNextEntry;
            myNextEntry = advanceToNextEntry();
            return entry;
        }

        @Override
        public boolean hasNext() {
            return myNextEntry != null;
        }

        private HashEntry advanceToNextEntry() {
            HashEntry myEntry = null;
            while (super.hasNext()) {
                myEntry = super.nextEntry();
                if (myEntry != null && !hideValue(myEntry)) {
                    break;
                } else {
                    myEntry = null;
                }
            }
            return myEntry;
        }

        protected boolean hideValue(HashEntry hashEntry) {
            return false;
        }
    }

    abstract class HashIterator {
        int nextSegmentIndex;
        int nextTableIndex;
        HashEntry[] currentTable;
        HashEntry nextEntry;
        HashEntry lastReturned;

        HashIterator() {
            nextSegmentIndex = segments.length - 1;
            nextTableIndex = -1;
            advance();
        }

        public boolean hasMoreElements() {
            return hasNext();
        }

        final void advance() {
            if (nextEntry != null && (nextEntry = nextEntry.next) != null)
                return;

            while (nextTableIndex >= 0) {
                if ((nextEntry = currentTable[nextTableIndex--]) != null)
                    return;
            }

            while (nextSegmentIndex >= 0) {
                Segment seg = segments[nextSegmentIndex--];
                if (seg.count != 0) {
                    currentTable = seg.table;
                    for (int j = currentTable.length - 1; j >= 0; --j) {
                        if ((nextEntry = currentTable[j]) != null) {
                            nextTableIndex = j - 1;
                            return;
                        }
                    }
                }
            }
        }

        public boolean hasNext() {
            return nextEntry != null;
        }

        HashEntry nextEntry() {
            if (nextEntry == null)
                throw new NoSuchElementException();
            lastReturned = nextEntry;
            advance();
            return lastReturned;
        }

        public void remove() {
            if (lastReturned == null)
                throw new IllegalStateException();
            SelectableConcurrentHashMap.this.remove(lastReturned.key);
            lastReturned = null;
        }
    }

    protected static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }
}