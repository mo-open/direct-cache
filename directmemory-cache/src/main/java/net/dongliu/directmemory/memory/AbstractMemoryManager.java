package net.dongliu.directmemory.memory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Ordering.from;

/**
 * Common methods for memory manager.
 * @author dongliu
 */
public abstract class AbstractMemoryManager implements MemoryManager {

    protected final Set<Pointer> pointers = Collections.newSetFromMap(new ConcurrentHashMap<Pointer, Boolean>());

    /** The mem size current used from allocator or something else */
    protected final AtomicLong usedMemory = new AtomicLong(0L);

    public Pointer store(byte[] payload, long expiresIn) {
        Pointer pointer = store(payload);
        if (pointer == null) {
            return pointer;
        }
        expire(pointer, expiresIn);
        return pointer;
    }

    @Override
    public Pointer store(byte[] payload, Date expiresTill) {
        Pointer pointer = store(payload);
        if (pointer == null) {
            return pointer;
        }
        expire(pointer, expiresTill);
        return pointer;
    }


    @Override
    public Pointer update(Pointer pointer, byte[] payload, long expiresIn) {
        Pointer newPointer = update(pointer, payload);
        if (newPointer == null) {
            return newPointer;
        }
        expire(newPointer, expiresIn);
        return newPointer;
    }

    @Override
    public Pointer update(Pointer pointer, byte[] payload, Date expirestill) {
        Pointer newPointer = update(pointer, payload);
        if (newPointer == null) {
            return newPointer;
        }
        expire(newPointer, expirestill);
        return newPointer;
    }

    @Override
    public long used() {
        return usedMemory.get();
    }

    @Override
    public void collectExpired() {
        int limit = 500;
        Iterator<Pointer> it = pointers.iterator();
        int count = 0;
        List<Pointer> expiredPointers = new ArrayList<Pointer>();
        while(it.hasNext()) {
            Pointer pointer = it.next();
            if (pointer.isExpired()) {
                expiredPointers.add(pointer);
                if (count++ > limit) {
                    break;
                }
            }
        }
        for (Pointer p : expiredPointers) {
            free(p);
        }
    }

    @Override
    public void collectLFU() {

        int limit = pointers.size() / 10;

        Iterable<Pointer> result = from(new Comparator<Pointer>() {
            @Override
            public int compare(Pointer o1, Pointer o2) {
                float f1 = o1.getFrequency();
                float f2 = o2.getFrequency();
                return Float.compare(f1, f2);
            }

        }).sortedCopy(limit(pointers, limit));

        free(result);
    }

    protected long free(Iterable<Pointer> pointers) {
        long howMuch = 0;
        for (Pointer expired : pointers) {
            howMuch += expired.getCapacity();
            free(expired);
        }
        return howMuch;
    }

    @Override
    public Set<Pointer> getPointers() {
        return Collections.unmodifiableSet(pointers);
    }

    @Override
    public void expire(Pointer pointer, long expiresIn) {
        pointer.setExpiration(pointer.created + expiresIn);
    }

    @Override
    public void expire(Pointer pointer, Date expirestill) {
        pointer.setExpiration(expirestill.getTime());
    }
}