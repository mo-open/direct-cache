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
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.dongliu.directcache.evict.Lru;
import net.dongliu.directcache.memory.Allocator;
import net.dongliu.directcache.struct.ValueWrapper;

/**
 * CacheHashMap subclasses a repackaged version of ConcurrentHashMap
 * ito allow efficient random sampling of the map values.
 * <p/>
 * The random sampling technique involves randomly selecting a map Segment, and then
 * selecting a number of random entry chains from that segment.
 */
public class CacheHashMap {

    /**
     * The maximum capacity, used if a higher node is implicitly specified by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30 to ensure that entries are indexable using ints.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The maximum number of segments to allow; used to bound constructor arguments.
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
     * Mask node for indexing into segments. The upper bits of a key's hash code are used to choose the segment.
     */
    private final int segmentMask;

    /**
     * Shift node for indexing within segments.
     */
    private final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table
     */
    private final Segment[] segments;

    private final CacheEventListener cacheEventListener;

    private Set<Object> keySet;
    private Set<Map.Entry<Object, ValueWrapper>> entrySet;
    private Collection<ValueWrapper> values;

    private final Allocator allocator;

    public CacheHashMap(Allocator allocator, int initialCapacity,
                        float loadFactor, int concurrency, final CacheEventListener cacheEventListener) {
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

        this.cacheEventListener = cacheEventListener;
    }


    /**
     * Returns the number of key-node mappings in this map without locking anything.
     * This may not give the exact element count as locking is avoided.
     * If the map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-node mappings in this map
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
     * set key - element. if has old node, also returnTo the old pointer.
     *
     * @param key
     * @param element
     * @return the old ValueWrapper, null if not exists
     */
    public ValueWrapper put(Object key, ValueWrapper element) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, element, false);
    }

    /**
     * set key - element if absent.
     *
     * @param key
     * @param element
     * @return
     */
    public ValueWrapper putIfAbsent(Object key, ValueWrapper element) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, element, true);
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

    /**
     * evict entries in the segement containing the key.
     */
    public int evict(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).evict();
    }

    protected Segment createSegment(int initialCapacity, float lf) {
        return new Segment(initialCapacity, lf);
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

    /**
     * Segments.
     */
    public class Segment extends ReentrantReadWriteLock {

        private static final int MAX_EVICTION = 5;

        /** The number of elements in this segment's region. */
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
         * (The node of this field is always <tt>(int)(capacity *
         * loadFactor)</tt>.)
         */
        int threshold;

        /**
         * The per-segment table.
         */
        protected volatile HashEntry[] table;

        /**
         * The load factor for the hash table.  Even though this node
         * is same for all segments, it is replicated to avoid needing
         * links to outer object.
         *
         * @serial
         */
        final float loadFactor;

        private final Lru lru = new Lru();

        private volatile int seq = 0;

        private Iterator<HashEntry> evictionIterator = iterator();

        protected Segment(int initialCapacity, float lf) {
            loadFactor = lf;
            setTable(new HashEntry[initialCapacity]);
        }

        /**
         * operations before delete from hashmap.
         * TODO: remove make value unusable.
         */
        protected void preRemove(HashEntry e) {
            lru.remove(e.node);
            e.node.getValue().returnTo(allocator);
        }

        /**
         * oprations after put.
         */
        protected void postInstall(Object key, Lru.Node node) {
            lru.add(node);
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
                                            ValueWrapper value) {
            return new HashEntry(key, hash, next, value);
        }

        protected HashEntry relinkHashEntry(HashEntry e, HashEntry next) {
            return new HashEntry(e.key, e.hash, next, e.node);
        }

        protected void clear() {
            writeLock().lock();
            try {
                if (count != 0) {
                    HashEntry[] tab = table;
                    for (int i = 0; i < tab.length; i++) {
                        tab[i].node.getValue().returnTo(allocator);
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
                    ValueWrapper v = e.node.getValue();
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
                    }
                }
                return oldValue;
            } finally {
                writeLock().unlock();
            }
        }

        protected ValueWrapper put(Object key, int hash, ValueWrapper value, boolean onlyIfAbsent) {
            writeLock().lock();
            try {
                int c = count;
                if (c++ > threshold) // ensure capacity
                    rehash();
                HashEntry[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry first = tab[index];
                HashEntry oldEntry = first;
                while (oldEntry != null && (oldEntry.hash != hash || !key.equals(oldEntry.key)))
                    oldEntry = oldEntry.next;

                ValueWrapper oldValue;
                if (oldEntry != null) {
                    oldValue = oldEntry.node.getValue();
                    if (!onlyIfAbsent) {
                        // replace
                        oldEntry.node = new Lru.Node(value);
                        postInstall(key, oldEntry.node);
                    } else {
                        return oldValue;
                    }
                } else {
                    // add
                    oldValue = null;
                    ++modCount;
                    tab[index] = createHashEntry(key, hash, first, value);
                    count = c; // write-volatile
                    postInstall(key, tab[index].node);
                }

                return oldValue;
            } finally {
                writeLock().unlock();
            }
        }

        private void notifyEvictionOrExpiry(final ValueWrapper element) {
            if (element != null && cacheEventListener != null) {
                if (element.isExpired()) {
                    cacheEventListener.notifyExpired(element, false);
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
                            if (seq++ % 10 == 0) {
                                // sample visit call to lru
                                lru.visit(e.node);
                            }
                            return e.node.getValue();
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
                            ValueWrapper v = e.node.getValue();
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

        protected Iterator<HashEntry> iterator() {
            return new SegmentIterator(this);
        }

        /**
         * evict by lru.
         * @return
         */
        private int evict() {
            int evictCount = 100;
            if (evictCount > this.count / 10) {
                evictCount = this.count / 10;
            }
            if (evictCount < 5) {
                evictCount = 5;
            }

            int count = 0;
            writeLock().lock();
            ReentrantLock lock = lru.getLock();
            lock.lock();
            try {
                Lru.Node[] nodes = lru.evict(evictCount);
                for (Lru.Node node : nodes) {
                    if (node == null) {
                        break;
                    }
                    Object key = node.getValue().getKey();
                    ValueWrapper remove = remove(key, hash(key.hashCode()), null);
                    notifyEvictionOrExpiry(remove);
                    count++;
                }
            } finally {
                lock.unlock();
                writeLock().unlock();
            }
            return count;
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

    /**
     * HashEntry.
     */
    public static class HashEntry {
        public final Object key;
        public final int hash;
        public final HashEntry next;

        public volatile Lru.Node node;

        protected HashEntry(Object key, int hash, HashEntry next, ValueWrapper node) {
            this.key = key;
            this.hash = hash;
            this.next = next;
            this.node = new Lru.Node(node);
        }

        protected HashEntry(Object key, int hash, HashEntry next, Lru.Node node) {
            this.key = key;
            this.hash = hash;
            this.next = next;
            this.node = node;
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
            throw new UnsupportedOperationException("Segment remove is not supported");
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
            return CacheHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return CacheHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return CacheHashMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            return CacheHashMap.this.remove(o) != null;
        }

        @Override
        public void clear() {
            CacheHashMap.this.clear();
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
            return CacheHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return CacheHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return CacheHashMap.this.containsValue(o);
        }

        @Override
        public void clear() {
            CacheHashMap.this.clear();
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
            return CacheHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return CacheHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry<?, ?> e = (Entry<?, ?>) o;
            ValueWrapper v = CacheHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry<?, ?> e = (Entry<?, ?>) o;
            return CacheHashMap.this.remove(e.getKey(), e.getValue());
        }

        @Override
        public void clear() {
            CacheHashMap.this.clear();
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
            return nextEntry().node.getValue();
        }
    }

    final class EntryIterator extends HashEntryIterator implements Iterator<Entry<Object, ValueWrapper>> {

        @Override
        public Entry<Object, ValueWrapper> next() {
            HashEntry entry = nextEntry();
            final Object key = entry.key;
            final ValueWrapper value = entry.node.getValue();
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
            CacheHashMap.this.remove(lastReturned.key);
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