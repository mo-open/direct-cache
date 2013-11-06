package net.dongliu.directcache.evict;

import net.dongliu.directcache.struct.ValueWrapper;

import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU implemetation.
 * TODO: less locks.
 * @author dongiu
 */
public class Lru {

    private volatile Node head;
    private volatile Node tail;

    private ReentrantLock lock = new ReentrantLock();

    public Node add(Node node) {
        lock.lock();
        try{
            if (head != null) {
                head.prev = node;
                node.next = head;
                head = node;
            } else {
                head = tail = node;
            }
            return node;
        } finally {
            lock.unlock();
        }
    }

    public void remove(Node node) {
        lock.lock();
        try {
            Node prev = node.prev;
            Node next = node.next;
            if (prev != null) {
                prev.next = node.next;
            } else {
                head = node.next;
            }
            if (next != null) {
                next.prev = node.prev;
            } else {
                tail = node.prev;
            }
        } finally {
            lock.unlock();
        }
    }

    public void visit(Node node) {
        lock.lock();
        try {
            if (node != head) {
                node.prev.next = node.next;
                if (node != tail) {
                    node.next.prev = node.prev;
                }
                node.next = head;
                node.prev = null;
                head.prev = node;
                head = node;
            }
        } finally {
            lock.unlock();
        }
    }

    public Node[] evict(int count) {
        lock.lock();
        try {
            Node[] nodes = new Node[count];
            //Note: the last node will remain.
            int i = 0;
            while (i <= count && tail.prev != null) {
                nodes[i] = tail;
                tail = tail.prev;
            }
            return nodes;
        } finally {
            lock.unlock();
        }
    }

    public ReentrantLock getLock() {
        return this.lock;
    }

    public static class Node {
        private final ValueWrapper value;
        private volatile Node prev;
        private volatile Node next;

        public Node (ValueWrapper value) {
            this.value = value;
        }

        public ValueWrapper getValue() {
            return this.value;
        }
    }
}
