package net.dongliu.direct.serialization;

import net.dongliu.commons.lang.IO;
import org.junit.Test;

import static org.junit.Assert.*;

public class DefaultSerializerTest {

    @Test
    public void testSerialize() throws Exception {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        DefaultSerializer<byte[]> serializer = new DefaultSerializer<>();
        byte[] bytes = serializer.serialize(data);
        byte[] newData = serializer.deSerialize(bytes, byte[].class);
        assertArrayEquals(data, newData);
    }

    @Test
    public void testStringSerialize() throws Exception {
        String str = "This is a test";
        DefaultSerializer<String> serializer = new DefaultSerializer<>();
        byte[] bytes = serializer.serialize(str);
        String str2 = serializer.deSerialize(bytes, String.class);
        assertEquals(str, str2);
    }
}