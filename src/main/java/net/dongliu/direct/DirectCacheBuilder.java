package net.dongliu.direct;

import net.dongliu.direct.serialization.HessianSerializer;
import sun.misc.VM;

/**
 * direct cache builder
 *
 * @author Dong Liu
 */
public class DirectCacheBuilder {

    /**
     * Cache concurrent map concurrent level
     */
    private int concurrency = 128;
    private long maxMemory = VM.maxDirectMemory() * 2 / 3;
    private Serializer serializer = new HessianSerializer();

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
        return new DirectCache(maxMemory, concurrency, serializer);
    }
}
