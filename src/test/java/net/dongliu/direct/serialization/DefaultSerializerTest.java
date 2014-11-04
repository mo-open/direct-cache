package net.dongliu.direct.serialization;

import net.dongliu.direct.Mock.Person;
import net.dongliu.direct.exception.DeSerializeException;
import net.dongliu.direct.exception.SerializeException;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DefaultSerializerTest {

    @Test
    public void testSerialize() throws Exception {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        DefaultSerializer serializer = new DefaultSerializer();
        byte[] bytes = serializer.serialize(data);
        byte[] newData = serializer.deSerialize(bytes, byte[].class);
        assertArrayEquals(data, newData);
    }

    @Test
    public void testStringSerialize() throws Exception {
        String str = "This is a test";
        DefaultSerializer serializer = new DefaultSerializer();
        byte[] bytes = serializer.serialize(str);
        String str2 = serializer.deSerialize(bytes, String.class);
        assertEquals(str, str2);
    }

    @Test
    public void testBean() throws SerializeException, DeSerializeException {
        DefaultSerializer serializer = new DefaultSerializer();
        Person person = new Person();
        byte[] bytes = serializer.serialize(person);
        Person person2 = serializer.deSerialize(bytes, Person.class);
        assertEquals(person.getAge(), person2.getAge());
        assertEquals(person.getSalary(), person2.getSalary());
    }
}