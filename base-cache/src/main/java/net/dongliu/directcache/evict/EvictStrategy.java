package net.dongliu.directcache.evict;

import net.dongliu.directcache.struct.ValueWrapper;

public interface EvictStrategy<T extends Node> {
    Node add(T node);

    void remove(T node);

    void visit(T node);

    T[] evict(int count);

    T newNode(ValueWrapper value);

    void clear();
}
