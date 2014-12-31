package net.dongliu.direct.Mock;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @author Dong Liu
 */
public class Person implements Serializable {

    private int age;
    private String name;
    private Date birth;
    private BigDecimal salary = new BigDecimal(100000);

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

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }
}
