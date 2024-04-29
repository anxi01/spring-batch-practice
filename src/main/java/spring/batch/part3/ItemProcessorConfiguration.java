package spring.batch.part3;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ItemProcessorConfiguration {

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;

  public ItemProcessorConfiguration(JobBuilderFactory jobBuilderFactory,
      StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean
  public Job itemProcessorJob() {
    return jobBuilderFactory.get("itemProcessorJob")
        .incrementer(new RunIdIncrementer())
        .start(this.itemProcessorStep())
        .build();
  }

  @Bean
  public Step itemProcessorStep() {
    return stepBuilderFactory.get("itemProcessorStep")
        .<Person, Person>chunk(10)
        .reader(itemReader())
        .processor(itemProcessor())
        .writer(itemWriter())
        .build();
  }

  private ItemWriter<Person> itemWriter() {
    return item -> item.forEach(s -> log.info("person id : {}", s.getId()));
  }

  private ItemProcessor<? super Person, ? extends Person> itemProcessor() {
    return item -> {
      if (item.getId() % 2 == 0) {
        return item;
      }
      return null;
    };
  }

  private ItemReader<Person> itemReader() {
    return new ListItemReader<>(getItems());
  }

  private List<Person> getItems() {
    List<Person> items = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      items.add(new Person(i + 1, "test name" + 1, "test age", "test address"));
    }

    return items;
  }


}
