package net.dongliu.directmemory.memory.allocator;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Utility class around direct {@link ByteBuffer}
 */
public class DirectByteBufferUtils {

    private static final sun.misc.Unsafe UNSAFE;

    private static final Field ADDRESS_FIELD;

    static {
        Object result = null;
        try {
            Class<?> clazz = Class.forName("sun.misc.Unsafe");
            for (Field field : clazz.getDeclaredFields()) {
                if (field.getType() == clazz &&(field.getModifiers() & (Modifier.FINAL | Modifier.STATIC))
                        == (Modifier.FINAL | Modifier.STATIC)) {
                    field.setAccessible(true);
                    result = field.get(null);
                    break;
                }
            }
        } catch (Throwable ignore) { }
        UNSAFE = result == null ? null : (sun.misc.Unsafe) result;
    }

    static {
        Field f;
        try {
            f = Buffer.class.getDeclaredField("address");
            f.setAccessible(true);
        } catch (Exception e) {
            f = null;
        }
        ADDRESS_FIELD = f;
    }

    /**
     * bytebuffer lack the important absulote-postion read & write method.
     * we make one...
     */
    public static void absolutePut(ByteBuffer buffer, int position, byte[] src, int offset, int length) {
        try {
            long dstAddress = (Long) ADDRESS_FIELD.get(buffer) + position;
            UNSAFE.copyMemory(src, UNSAFE.arrayBaseOffset(byte[].class) + offset, null, dstAddress, length);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * bytebuffer lack the important absulote-postion read & write method.
     * we make one...
     */
    public static void absoluteGet(ByteBuffer buffer, int position, byte[] src, int offset, int length) {
        try {
            long dstAddress = (Long) ADDRESS_FIELD.get(buffer) + position;
            UNSAFE.copyMemory(null, dstAddress, src, UNSAFE.arrayBaseOffset(byte[].class) + offset,length);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Clear and release the internal content of a direct {@link ByteBuffer}.
     * Clearing manually the content avoid waiting till the GC do his job.
     *
     * @param buffer : the buffer to clear
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public static void destroyDirectByteBuffer(final ByteBuffer buffer)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException,
            NoSuchMethodException {

        Method cleanerMethod = buffer.getClass().getMethod("cleaner");
        cleanerMethod.setAccessible(true);
        Object cleaner = cleanerMethod.invoke(buffer);
        Method cleanMethod = cleaner.getClass().getMethod("clean");
        cleanMethod.setAccessible(true);
        cleanMethod.invoke(cleaner);

    }

}
