package spring.batch.part3;

import java.util.ArrayList;
import java.util.List;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

/**
 * Java Collection의 List를 Reader로 처리하는 클래스
 */
public class CustomItemReader<T> implements ItemReader<T> {

  private final List<T> items;

  public CustomItemReader(List<T> items) {
    this.items = new ArrayList<>(items);
  }

  @Override
  public T read()
      throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
    if (!items.isEmpty()) {
      return items.remove(0);
    }

    return null; // null을 return하면 chunk 반복이 끝난다는 의미.
  }
}
