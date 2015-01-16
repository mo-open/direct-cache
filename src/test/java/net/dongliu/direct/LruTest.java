package net.dongliu.direct;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LruTest {

    int promoteDelta = 1_000;

    @Test
    public void testInsert() throws Exception {
        Lru lru = mockLru(0);
        DirectValue node = new DirectValue(null, null, null);
        lru.insert(node);
        assertTrue(node == lru.getHead());
        assertTrue(node == lru.getTail());
        DirectValue node2 = new DirectValue(null, null, null);
        lru.insert(node2);
        assertTrue(node2 == lru.getHead());
        assertTrue(node == lru.getTail());

        assertEquals(2, lru.tails(10).size());
    }

    @Test
    public void testRemove() throws Exception {
        Lru lru = mockLru(2);
        DirectValue node = new DirectValue(null, null, null);
        lru.insert(node);
        lru.remove(node);
        assertTrue(node != lru.getHead());

        assertEquals(2, lru.tails(10).size());
    }

    @Test
    public void testPromoted() throws Exception {
        Lru lru = mockLru(2);
        DirectValue node = new DirectValue(null, null, null);
        DirectValue node2 = new DirectValue(null, null, null);
        lru.insert(node);
        lru.insert(node2);
        lru.promoted(node);
        assertTrue(node != lru.getHead());
        Thread.sleep(1_000);
        lru.promoted(node);
        assertTrue(node == lru.getHead());

        assertEquals(4, lru.tails(10).size());
    }

    @Test
    public void testPopTail() throws Exception {
        Lru lru = mockLru(5);
        List<DirectValue> nodes = lru.tails(10);
        assertEquals(5, nodes.size());
    }

    private Lru mockLru(int size) {
        Lru lru = new Lru(promoteDelta);
        for (int i = 0; i < size; i++) {
            lru.insert(new DirectValue(null, null, null));
        }
        return lru;
    }
}