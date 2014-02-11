package net.dongliu.direct.evict;

import net.dongliu.direct.struct.ValueHolder;

public interface EvictStrategy<T extends Node> {
    Node add(T node);

    void remove(T node);

    void visit(T node);

    T[] evict(int count);

    T newNode(ValueHolder value);

    void clear();
}
