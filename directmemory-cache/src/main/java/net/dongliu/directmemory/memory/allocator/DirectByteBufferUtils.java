package net.dongliu.directmemory.memory.allocator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class around direct {@link ByteBuffer}
 */
public class DirectByteBufferUtils {

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

        checkArgument(buffer.isDirect(), "toBeDestroyed isn't direct!");

        Method cleanerMethod = buffer.getClass().getMethod("cleaner");
        cleanerMethod.setAccessible(true);
        Object cleaner = cleanerMethod.invoke(buffer);
        Method cleanMethod = cleaner.getClass().getMethod("clean");
        cleanMethod.setAccessible(true);
        cleanMethod.invoke(cleaner);

    }

}
