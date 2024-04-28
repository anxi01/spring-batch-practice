package spring.batch.part3;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@Slf4j
public class ItemReaderConfiguration {

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;

  public ItemReaderConfiguration(JobBuilderFactory jobBuilderFactory,
      StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean
  public Job itemReaderJob() throws Exception {
    return jobBuilderFactory.get("itemReaderJob")
        .incrementer(new RunIdIncrementer())
        .start(this.customItemReaderStep())
        .next(this.csvFileStep())
        .build();
  }

  @Bean
  public Step customItemReaderStep() {
    return stepBuilderFactory.get("customItemReaderStep")
        .<Person, Person>chunk(10)
        .reader(new CustomItemReader<>(getItems()))
        .writer(itemWriter())
        .build();
  }

  @Bean
  public Step csvFileStep() throws Exception {
    return stepBuilderFactory.get("csvFileStep")
        .<Person, Person>chunk(10)
        .reader(csvFileItemReader())
        .writer(itemWriter())
        .build();
  }

  private FlatFileItemReader<Person> csvFileItemReader() throws Exception {
    DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
    DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
    tokenizer.setNames("id", "name", "age", "address");
    lineMapper.setLineTokenizer(tokenizer);

    lineMapper.setFieldSetMapper(fieldSet -> {
      int id = fieldSet.readInt("id");
      String name = fieldSet.readString("name");
      String age = fieldSet.readString("age");
      String address = fieldSet.readString("address");

      return new Person(id, name, age, address);
    });

    FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
        .name("csvFileItemReader")
        .encoding("UTF-8")
        .resource(new ClassPathResource("test.csv"))
        .linesToSkip(1) // csv의 첫 번째 로우인 필드명을 스킵하겠다는 의미
        .lineMapper(lineMapper)
        .build();
    itemReader.afterPropertiesSet(); // itemReader에서 필수 설정 값이 정상적으로 설정됐는지 확인해주는 메서드

    return itemReader;
  }

  private ItemWriter<Person> itemWriter() {
    return items -> log.info(items.stream()
        .map(Person::getName)
        .collect(Collectors.joining(", ")));
  }

  private List<Person> getItems() {
    List<Person> items = new ArrayList<>();

    for(int i = 0; i < 10; i++) {
      items.add(new Person(i + 1, "test name" + i, "test age", "test address"));
    }

    return items;
  }
}
