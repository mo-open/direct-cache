package net.dongliu.direct;

import net.dongliu.direct.serialization.HessianSerializer;
import net.dongliu.direct.utils.Size;

/**
 * direct cache builder
 *
 * @author Dong Liu
 */
public class DirectCacheBuilder {

    /**
     * Cache concurrent map concurrent level
     */
    private int concurrency = 256;


    private long maxMemory = Size.Gb(1);

    private Serializer serializer;

    DirectCacheBuilder() {
    }

    public DirectCacheBuilder concurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    public DirectCacheBuilder maxMemorySize(long maxMemorySize) {
        this.maxMemory = maxMemorySize;
        return this;
    }

    private DirectCacheBuilder serializer(Serializer serializer) {
        this.serializer = serializer;
        return this;
    }

    public DirectCache build() {
        if (serializer == null) {
            serializer = new HessianSerializer();
        }
        return new DirectCache(maxMemory, concurrency, serializer);
    }
}
