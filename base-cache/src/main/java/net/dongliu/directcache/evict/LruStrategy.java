package net.dongliu.directcache.evict;

import net.dongliu.directcache.struct.ValueWrapper;

import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU implemetation.
 * TODO: less locks.
 * @author dongiu
 */
public class LruStrategy implements EvictStrategy<LruStrategy.LruNode> {

    private LruNode head;
    private LruNode tail;

    private volatile int seq = 0;

    private ReentrantLock lock = new ReentrantLock();

    @Override
    public net.dongliu.directcache.evict.Node add(LruNode node) {
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

    @Override
    public void remove(LruNode node) {
        lock.lock();
        try {
            LruNode prev = node.prev;
            LruNode next = node.next;
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

    @Override
    public void visit(LruNode node) {
        if (seq++ % 10 != 0) {
            return;
        }
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

    @Override
    public LruNode[] evict(int count) {
        lock.lock();
        try {
            LruNode[] nodes = new LruNode[count];
            //Note: the last node will remain.
            int i = 0;
            LruNode node = tail;
            while (i <= count && node.prev != null) {
                nodes[i] = node;
                node = node.prev;
            }
            return nodes;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public LruNode newNode(ValueWrapper value) {
        return new LruNode(value);
    }

    protected static class LruNode implements net.dongliu.directcache.evict.Node {
        private final ValueWrapper value;
        private volatile LruNode prev;
        private volatile LruNode next;

        public LruNode(ValueWrapper value) {
            this.value = value;
        }

        @Override
        public ValueWrapper getValue() {
            return this.value;
        }
    }
}
