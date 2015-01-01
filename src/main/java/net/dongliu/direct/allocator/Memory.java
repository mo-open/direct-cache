package net.dongliu.direct.allocator;

/**
 * @author Dong Liu dongliu@wandoujia.com
 */
public class Memory {
    private final long address;
    private final int size;


    public Memory(long address, int size) {
        this.address = address;
        this.size = size;
    }

    public long getAddress() {
        return address;
    }

    public int getSize() {
        return size;
    }
}
