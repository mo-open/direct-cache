package net.dongliu.directmemory.memory.allocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractByteBufferAllocator implements Allocator {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected final Logger getLogger() {
        return logger;
    }

    protected final boolean isClosed() {
        return closed.get();
    }

    protected final void setClosed(final boolean closed) {
        this.closed.set(closed);
    }

    protected static Integer getHash(final ByteBuffer buffer) {
        return System.identityHashCode(buffer);
    }

}
