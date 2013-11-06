package net.dongliu.directcache.evict;

import net.dongliu.directcache.struct.ValueWrapper;

/**
 * LRU implemetation.
 * TODO: less locks.
 * @author dongiu
 */
public class Lru {

    private volatile Node head;
    private volatile Node tail;

    public synchronized Node add(Node node) {
        if (head != null) {
            head.prev = node;
            node.next = head;
            head = node;
        } else {
            head = tail = node;
        }
        return node;
    }

    public synchronized void remove(Node node) {
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
    }

    public synchronized void visit(Node node) {
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
    }

    public synchronized Node[] evict(int count) {
        Node[] nodes = new Node[count];
        //Note: the last node will remain.
        int i = 0;
        while (i <= count && tail.prev != null) {
            nodes[i] = tail;
            tail = tail.prev;
        }
        return nodes;
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
