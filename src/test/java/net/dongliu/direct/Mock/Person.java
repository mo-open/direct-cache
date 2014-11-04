package net.dongliu.direct.Mock;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Dong Liu
 */
public class Person implements Serializable {

    private int age;
    private String name;
    private Date birth;

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getBirth() {
        return birth;
    }

    public void setBirth(Date birth) {
        this.birth = birth;
    }
}
