package net.dongliu.direct.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * configures. load from configure file direct-cache.properties if any
 *
 * @author dongliu
 */
public class CacheConfigure {

    /**
     * Cache concurrent map concurrent level
     */
    private int concurrency = 256;

    /**
     * Cache concurrent map initial size of one segement.
     */
    private int initialSize = 1024;

    /**
     * cache map load factor
     */
    private float loadFactor = 0.75f;

    /**
     * min chunk size for slab allocator
     */
    private int minEntySize = 48;

    /**
     * max chunk size for slab allocator. It also determine how much the cache data can be.
     */
    private int maxEntrySize = Size.Mb(16);

    /**
     * chunk expand factor.
     */
    private float expandFactor = 1.25f;

    /**
     * the serializer class
     */
    private String serializerClass = null;

    private static CacheConfigure instance = new CacheConfigure();

    private static final String CONFIGURE_FILE = "direct-cache.properties";

    private static Logger logger = LoggerFactory.getLogger(CacheConfigure.class);

    private CacheConfigure() {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(CONFIGURE_FILE);
        if (in != null) {
            Properties p = new Properties();
            try {
                p.load(in);
            } catch (IOException ignore) {
            }

            concurrency = getInt(p, "cache.map.concurrency", concurrency);
            initialSize = getInt(p, "cache.map.initialSize", initialSize);

            minEntySize = getInt(p, "cache.slab.minSize", minEntySize);
            maxEntrySize = getInt(p, "cache.slab.maxSize", maxEntrySize);
            expandFactor = getFloat(p, "cache.slab.expand", expandFactor);

            serializerClass = p.getProperty("cache.serializer");
        }
    }

    private int getInt(Properties p, String name, int defaultValue) {
        try {
            return Integer.parseInt(p.getProperty(name, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            logger.warn("config " + name + " is not a valid int value.");
            return defaultValue;
        }
    }

    private float getFloat(Properties p, String name, float defaultValue) {
        try {
            return Float.parseFloat(p.getProperty(name, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            logger.warn("config " + name + " is not a valid float value.");
            return defaultValue;
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

    public float getExpandFactor() {
        return expandFactor;
    }

    public int getMaxEntrySize() {
        return maxEntrySize;
    }

    public int getMinEntySize() {
        return minEntySize;
    }
}
