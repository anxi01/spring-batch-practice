package spring.batch.part3;

import lombok.Getter;

@Getter
public class Person {

  private String name;
  private int age;
  private String sex;

  public Person(String name, int age, String sex) {
    this.name = name;
    this.age = age;
    this.sex = sex;
  }
}
