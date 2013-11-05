package net.dongliu.directcache.serialization;

import static java.lang.String.format;
import static java.util.ServiceLoader.load;

public final class SerializerFactory {

    public static Serializer createNewSerializer() {
        return createNewSerializer(SerializerFactory.class.getClassLoader());
    }

    public static Serializer createNewSerializer(ClassLoader classLoader) {

        // iterate over all found services
        for (Serializer serializer : load(Serializer.class, classLoader)) {
            // try getting the current service and return
            try {
                return serializer;
            } catch (Throwable t) {
                // just ignore, skip and try getting the next
            }
        }

        return new StandardSerializer();
    }

    public static <S extends Serializer> S createNewSerializer(Class<S> serializer)
            throws SerializerNotFoundException {

        // iterate over all found services
        for (Serializer serializer1 : load(Serializer.class, serializer.getClassLoader())) {
            // try getting the current service and return
            try {
                Serializer next = serializer1;
                if (serializer.isInstance(next)) {
                    return serializer.cast(next);
                }
            } catch (Throwable t) {
                // just ignore, skip and try getting the next
            }
        }

        throw new SerializerNotFoundException(serializer);
    }

    public static Serializer createNewSerializer(String serializerClassName)
            throws SerializerNotFoundException {
        return createNewSerializer(serializerClassName, SerializerFactory.class.getClassLoader());
    }

    public static Serializer createNewSerializer(String serializerClassName, ClassLoader classLoader)
            throws SerializerNotFoundException {
        Class<?> anonSerializerClass;
        try {
            anonSerializerClass = classLoader.loadClass(serializerClassName);
        } catch (ClassNotFoundException e) {
            throw new SerializerNotFoundException(serializerClassName);
        }

        if (Serializer.class.isAssignableFrom(anonSerializerClass)) {
            @SuppressWarnings("unchecked") // the assignment is guarded by the previous check
                    Class<? extends Serializer> serializerClass = (Class<? extends Serializer>) anonSerializerClass;

            return createNewSerializer(serializerClass);
        }

        throw new IllegalArgumentException(format("Class %s is not a valid Serializer type",
                anonSerializerClass.getName()));
    }

    /**
     * Hidden constructor, this class cannot be instantiated
     */
    private SerializerFactory() {
        // do nothing
    }

}
