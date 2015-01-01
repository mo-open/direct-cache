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
    private int concurrency = 256;
    private long maxMemory = -1;
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
        if (maxMemory < 0) {
            maxMemory = VM.maxDirectMemory() * 2 / 3;
        }
        return new DirectCache(maxMemory, concurrency, serializer);
    }
}
