package net.dongliu.direct.serialization;

import net.dongliu.direct.Mock.Person;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class HessianSerializerTest {

    @Test
    public void testSerialize() throws Exception {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        HessianSerializer serializer = new HessianSerializer();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        serializer.serialize(data, bos);
        byte[] bytes = bos.toByteArray();
        byte[] newData = serializer.deSerialize(new ByteArrayInputStream(bytes), byte[].class);
        assertArrayEquals(data, newData);
    }

    @Test
    public void testStringSerialize() throws Exception {
        String str = "This is a test";
        HessianSerializer serializer = new HessianSerializer();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        serializer.serialize(str, bos);
        byte[] bytes = bos.toByteArray();
        String str2 = serializer.deSerialize(new ByteArrayInputStream(bytes), String.class);
        assertEquals(str, str2);
    }

    @Test
    public void testBean() throws SerializeException, DeSerializeException, IOException {
        HessianSerializer serializer = new HessianSerializer();
        Person person = new Person();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        serializer.serialize(person, bos);
        byte[] bytes = bos.toByteArray();
        Person person2 = serializer.deSerialize(new ByteArrayInputStream(bytes), Person.class);
        assertEquals(person.getAge(), person2.getAge());
        assertEquals(person.getSalary(), person2.getSalary());
    }
}