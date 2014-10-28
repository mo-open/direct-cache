package net.dongliu.direct.cache;

import net.dongliu.direct.serialization.DefaultSerializer;
import net.dongliu.direct.serialization.Serializer;
import net.dongliu.direct.utils.Size;

/**
 * @author Dong Liu
 */
public class DirectCacheBuilder<K, V> {

    /**
     * Cache concurrent map concurrent level
     */
    private int concurrency = 256;

    /**
     * Cache concurrent map initial size of one segment.
     */
    private int initialSize = 1024;

    /**
     * cache map load factor
     */
    private float loadFactor = 0.75f;

    /**
     * min chunk size for slab allocator
     */
    private int chunkSize = 48;

    /**
     * chunk expand factor.
     */
    private float expandFactor = 1.25f;

    private long maxMemorySize = Size.Gb(1);

    private int slabSize = Size.Mb(4);

    private Serializer<V> serializer = new DefaultSerializer<>();

    DirectCacheBuilder() {
    }

    public DirectCacheBuilder<K, V> concurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    public DirectCacheBuilder<K, V> initialSize(int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public DirectCacheBuilder<K, V> loadFactor(float loadFactor) {
        this.loadFactor = loadFactor;
        return this;
    }

    public DirectCacheBuilder<K, V> chunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public DirectCacheBuilder<K, V> slabSize(int slabSize) {
        this.slabSize = slabSize;
        return this;
    }

    public DirectCacheBuilder<K, V> expandFactor(float expandFactor) {
        this.expandFactor = expandFactor;
        return this;
    }

    public DirectCacheBuilder<K, V> maxMemorySize(long maxMemorySize) {
        this.maxMemorySize = maxMemorySize;
        return this;
    }

    private DirectCacheBuilder<K, V> serializer(Serializer<V> serializer) {
        this.serializer = serializer;
        return this;
    }

    public DirectCache<K, V> build() {
        return new DirectCache<>(maxMemorySize, expandFactor, chunkSize, slabSize,
                initialSize, loadFactor, concurrency, serializer);
    }
}
