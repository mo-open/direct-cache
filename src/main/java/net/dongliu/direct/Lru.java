package net.dongliu.direct;

import net.dongliu.direct.value.DirectValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * lru impl with deque
 *
 * @author Dong Liu dongliu@wandoujia.com
 */
class Lru {

    private Node head;
    private Node tail;

    private Lock lock = new ReentrantLock();

    /**
     * after promoteDelta ms do next promote
     */
    private static final long DEFAULT_PROMOTE_DELTA = 10_000;

    public final long promoteDelta;

    Lru() {
        this(DEFAULT_PROMOTE_DELTA);
    }

    Lru(long promoteDelta) {
        this.promoteDelta = promoteDelta;
    }

    /**
     * insert one node
     */
    void insert(Node node) {
        lock.lock();
        try {
            if (head == null) {
                head = tail = node;
            } else {
                node.successor = head;
                head.precursor = node;
                head = node;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * remove one node
     */
    void remove(Node node) {
        lock.lock();
        try {
            //head must not be null
            if (node == head) {
                head = node.successor;
                if (node == tail) {
                    tail = node.precursor;
                } else {
                    node.successor.precursor = null;
                }
            } else if (node == tail) {
                tail = node.precursor;
                node.precursor.successor = null;
            } else {
                node.successor.precursor = node.precursor;
                node.precursor.successor = node.successor;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * move node to head
     */
    void promoted(Node node) {
        long now = System.currentTimeMillis();
        long last = node.getLastPromoted();
        if (now < last + promoteDelta) {
            return;
        }

        if (node.compareAndSetLastPromoted(last, now)) {
            lock.lock();
            try {
                if (node == head) {
                    return;
                }
                head.precursor = node;
                node.precursor.successor = node.successor;
                if (node.successor != null) {
                    node.successor.precursor = node.precursor;
                }
                node.successor = head;
                node.precursor = null;
                head = node;
            } finally {
                lock.unlock();
            }
        }

    }

    /**
     * evict num node from list tail
     */
    List<Node> tails(int num) {
        List<Node> list = new ArrayList<>(num);
        lock.lock();
        try {
            Node node = tail;
            while (node != null) {
                list.add(node);
                node = node.precursor;
            }
            return list;
        } finally {
            lock.unlock();
        }
    }

    Node getHead() {
        return head;
    }

    Node getTail() {
        return tail;
    }

    static class Node {
        private Node successor;
        private Node precursor;
        private volatile long lastPromoted;
        volatile DirectValue value;

        private static final AtomicLongFieldUpdater<Node> updater
                = AtomicLongFieldUpdater.newUpdater(Node.class, "lastPromoted");

        Node(DirectValue value) {
            this.value = value;
            this.lastPromoted = System.currentTimeMillis();
        }

        public long getLastPromoted() {
            return lastPromoted;
        }

        public boolean compareAndSetLastPromoted(long expect, long update) {
            return updater.compareAndSet(this, expect, update);
        }

        public DirectValue getValue() {
            return value;
        }
    }

}

