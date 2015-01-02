package net.dongliu.direct;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LruTest {

    @Test
    public void testInsert() throws Exception {
        Lru lru = mockLru(0);
        Lru.Node node = new Lru.Node(null);
        lru.insert(node);
        assertTrue(node == lru.getHead());
        assertTrue(node == lru.getTail());
        Lru.Node node2 = new Lru.Node(null);
        lru.insert(node2);
        assertTrue(node2 == lru.getHead());
        assertTrue(node == lru.getTail());

        assertEquals(2, lru.tails(10).size());
    }

    @Test
    public void testRemove() throws Exception {
        Lru lru = mockLru(2);
        Lru.Node node = new Lru.Node(null);
        lru.insert(node);
        lru.remove(node);
        assertTrue(node != lru.getHead());

        assertEquals(2, lru.tails(10).size());
    }

    @Test
    public void testPromoted() throws Exception {
        Lru lru = mockLru(2);
        Lru.Node node = new Lru.Node(null);
        Lru.Node node2 = new Lru.Node(null);
        lru.insert(node);
        lru.insert(node2);
        lru.promoted(node);
        assertTrue(node != lru.getHead());
        Thread.sleep(2500);
        lru.promoted(node);
        assertTrue(node == lru.getHead());

        assertEquals(4, lru.tails(10).size());
    }

    @Test
    public void testPopTail() throws Exception {
        Lru lru = mockLru(5);
        List<Lru.Node> nodes = lru.tails(10);
        assertEquals(5, nodes.size());
    }

    private Lru mockLru(int size) {
        Lru lru = new Lru();
        for (int i = 0; i < size; i++) {
            lru.insert(new Lru.Node(null));
        }
        return lru;
    }
}