package spring.batch.part3;

import lombok.extern.log4j.Log4j2;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

@Log4j2
public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {

  private final RetryTemplate retryTemplate;

  public PersonValidationRetryProcessor() {
    this.retryTemplate = new RetryTemplateBuilder()
        // NotFoundNameException이 3번 발생할 때까지 재시도
        .maxAttempts(3)
        .retryOn(NotFoundNameException.class)
        .withListener(new SavePersonRetryListener())
        .build();
  }

  @Override
  public Person process(Person item) throws Exception {
    return this.retryTemplate.execute(context -> {
      // RetryCallback : RetryTemplate의 시작점(processor가 시작할 때)
      // RetryCallback이 3번 호출된 후에 RecoveryCallback이 발생

      if (item.isNotEmptyName()) {
        return item;
      }
      throw new NotFoundNameException();
    }, context -> {
      // RecoveryCallback
      return item.unknownName();
    });
  }

  public static class SavePersonRetryListener implements RetryListener {


    @Override
    public <T, E extends Throwable> boolean open(RetryContext context,
        RetryCallback<T, E> callback) {
      return true;
    }

    @Override
    public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
        Throwable throwable) {
      log.info("close");
    }

    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
        Throwable throwable) {
      // NotFoundNameException이 발생했을 때 동작
      log.error("onError");
    }
  }
}
