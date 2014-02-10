package net.dongliu.direct.memory;

import net.dongliu.direct.utils.U;

/**
 * Memory actualUsed unsafe ops.
 *
 * @author: dongliu
 */
public class UnsafeMemory {

    private final long address;
    private final int size;

    private UnsafeMemory(int size) {
        this.size = size;
        this.address = U.allocateMemory(size);
    }

    public static UnsafeMemory allocate(int size) {
        return new UnsafeMemory(size);
    }

    public void write(long pos, byte[] src, int offset, int size) {
        U.write(address + pos, src, offset, size);
    }

    public void write(long pos, byte[] src, int size) {
        U.write(address + pos, src, 0, size);
    }

    public void write(long pos, byte[] src) {
        U.write(address + pos, src, 0, src.length);
    }

    public void read(long pos, byte[] src, int offset, int size) {
        U.read(address + pos, src, offset, size);
    }

    public void read(long pos, byte[] src, int size) {
        U.read(address + pos, src, 0, size);
    }

    public void read(long pos, byte[] src) {
        U.read(address + pos, src, 0, src.length);
    }

    public void dispose() {
        U.freeMemory(address);
    }

    public int getSize() {
        return size;
    }
}
