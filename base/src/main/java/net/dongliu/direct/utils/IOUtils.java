package net.dongliu.direct.utils;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author: dongliu
 */
public class IOUtils {

    public static void closeQueitly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignore) {
            }
        }
    }

}
