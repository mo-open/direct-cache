package net.dongliu.direct.serialization;

import net.dongliu.direct.exception.SerializerNotFoundException;
import net.dongliu.direct.utils.CacheConfigure;

public final class SerializerFactory {

    private static final Serializer serializer;

    static {
        CacheConfigure configure = CacheConfigure.getConfigure();
        String serializerName = configure.getSerializerClass();
        if (serializerName == null || serializerName.equals("")) {
            serializer = new DefaultSerializer();
        } else {
            try {
                Class<?> serializerClass = Class.forName(configure.getSerializerClass());
                if (Serializer.class.isAssignableFrom(serializerClass) && !serializerClass.isInterface()) {
                    serializer = (Serializer) serializerClass.newInstance();
                } else {
                    serializer = new DefaultSerializer();
                }
            } catch (ClassNotFoundException e) {
                throw new SerializerNotFoundException(e);
            } catch (InstantiationException e) {
                throw new SerializerNotFoundException(e);
            } catch (IllegalAccessException e) {
                throw new SerializerNotFoundException(e);
            }
        }
    }

    public static Serializer getSerializer() {
        return serializer;
    }

}
