package net.dongliu.direct;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * lru impl with deque
 *
 * @author Dong Liu dongliu@wandoujia.com
 */
class Lru {

    private DirectValue head;
    private DirectValue tail;

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
     * insert one DirectValue
     */
    void insert(DirectValue value) {
        value.setLastPromoted(System.currentTimeMillis());
        lock.lock();
        try {
            if (head == null) {
                head = tail = value;
            } else {
                value.successor = head;
                head.precursor = value;
                head = value;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * remove one DirectValue
     */
    void remove(DirectValue DirectValue) {
        lock.lock();
        try {
            //head must not be null
            if (DirectValue == head) {
                head = DirectValue.successor;
                if (DirectValue == tail) {
                    tail = DirectValue.precursor;
                } else {
                    DirectValue.successor.precursor = null;
                }
            } else if (DirectValue == tail) {
                tail = DirectValue.precursor;
                DirectValue.precursor.successor = null;
            } else {
                DirectValue.successor.precursor = DirectValue.precursor;
                DirectValue.precursor.successor = DirectValue.successor;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * move DirectValue to head
     */
    void promoted(DirectValue DirectValue) {
        long now = System.currentTimeMillis();
        long last = DirectValue.getLastPromoted();
        if (now < last + promoteDelta) {
            return;
        }

        if (DirectValue.compareAndSetLastPromoted(last, now)) {
            lock.lock();
            try {
                if (DirectValue == head) {
                    return;
                }
                head.precursor = DirectValue;
                DirectValue.precursor.successor = DirectValue.successor;
                if (DirectValue.successor != null) {
                    DirectValue.successor.precursor = DirectValue.precursor;
                }
                DirectValue.successor = head;
                DirectValue.precursor = null;
                head = DirectValue;
            } finally {
                lock.unlock();
            }
        }

    }

    /**
     * evict num DirectValue from list tail
     */
    List<DirectValue> tails(int num) {
        List<DirectValue> list = new ArrayList<>(num);
        lock.lock();
        try {
            DirectValue DirectValue = tail;
            while (DirectValue != null) {
                list.add(DirectValue);
                DirectValue = DirectValue.precursor;
            }
            return list;
        } finally {
            lock.unlock();
        }
    }

    DirectValue getHead() {
        return head;
    }

    DirectValue getTail() {
        return tail;
    }

}

