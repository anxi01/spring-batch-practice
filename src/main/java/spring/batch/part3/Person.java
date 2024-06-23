package spring.batch.part3;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.util.Objects;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
public class Person {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private int id;
  private String name;
  private String age;
  private String address;

  public Person(String name, String age, String address) {
    this(0, name, age, address);
  }

  public Person(int id, String name, String age, String address) {
    this.id = id;
    this.name = name;
    this.age = age;
    this.address = address;
  }

  public boolean isNotEmptyName() {
    return Objects.nonNull(this.name) && !this.name.isEmpty();
  }
}
