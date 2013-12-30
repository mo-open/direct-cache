package net.dongliu.directcache.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author: dongliu
 */
public class CacheConfigure {

    /**
     * Cache concurrent map concurrent level
     */
    private int concurrency = 256;

    /**
     * Cache concurrent map initial size of one segement.
     */
    private int initialSize = 1000;

    /**
     * cache map load factor
     */
    private float loadFactor = 0.75f;

    /**
     * the serializer class
     */
    private String serializerClass = null;

    private static CacheConfigure instance = new CacheConfigure();

    private static final String CONFIGURE_FILE = "direct-cache.properties";

    private CacheConfigure() {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(CONFIGURE_FILE);
        if (in != null) {
            Properties p = new Properties();
            try {
                p.load(in);
            } catch (IOException ignore) {
            }
            concurrency = Integer.parseInt(p.getProperty("concurrency"));
            initialSize = Integer.parseInt(p.getProperty("initialSize"));
            serializerClass = p.getProperty("serializer");
        }
    }

    public static CacheConfigure getConfigure() {
        return instance;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public int getInitialSize() {
        return initialSize;
    }

    public String getSerializerClass() {
        return serializerClass;
    }

    public float getLoadFactor() {
        return loadFactor;
    }
}
