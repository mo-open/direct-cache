package net.dongliu.direct.cache;

import net.dongliu.direct.serialization.DefaultSerializer;
import net.dongliu.direct.serialization.Serializer;
import net.dongliu.direct.utils.Size;

/**
 * direct cache builder
 *
 * @author Dong Liu
 */
public class DirectCacheBuilder<K, V> {

    /**
     * Cache concurrent map concurrent level
     */
    private int concurrency = 256;


    private long maxMemory = Size.Gb(1);

    private Serializer serializer = new DefaultSerializer();

    DirectCacheBuilder() {
    }

    public DirectCacheBuilder<K, V> concurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    public DirectCacheBuilder<K, V> maxMemorySize(long maxMemorySize) {
        this.maxMemory = maxMemorySize;
        return this;
    }

    private DirectCacheBuilder<K, V> serializer(Serializer serializer) {
        this.serializer = serializer;
        return this;
    }

    public DirectCache<K, V> build() {
        return new DirectCache<>(maxMemory, concurrency, serializer);
    }
}
