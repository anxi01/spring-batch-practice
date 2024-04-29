package spring.batch.part3;

import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
@Slf4j
public class ItemWriterConfiguration {

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private final DataSource dataSource;
  private final EntityManagerFactory entityManagerFactory;

  public ItemWriterConfiguration(JobBuilderFactory jobBuilderFactory,
      StepBuilderFactory stepBuilderFactory, DataSource dataSource,
      EntityManagerFactory entityManagerFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
    this.dataSource = dataSource;
    this.entityManagerFactory = entityManagerFactory;
  }

  @Bean
  public Job itemWriterJob() throws Exception {
    return jobBuilderFactory.get("itemWriterJob")
        .incrementer(new RunIdIncrementer())
        .start(this.csvItemWriterStep())
//        .next(this.jdbcBatchItemWriterStep())
        .next(this.jpaItemWriterStep())
        .build();
  }

  @Bean
  public Step csvItemWriterStep() throws Exception {
    return stepBuilderFactory.get("csvItemWriterStep")
        .<Person, Person>chunk(10)
        .reader(itemReader())
        .writer(csvFileItemWriter())
        .build();
  }

  @Bean
  public Step jdbcBatchItemWriterStep() throws Exception {
    return stepBuilderFactory.get("jdbcBatchItemWriterStep")
        .<Person, Person>chunk(10)
        .reader(itemReader())
        .writer(jdbcBatchItemWriter())
        .build();
  }

  @Bean
  public Step jpaItemWriterStep() throws Exception {
    return stepBuilderFactory.get("jpaItemWriterStep")
        .<Person, Person>chunk(10)
        .reader(itemReader())
        .writer(jpaItemWriter())
        .build();
  }

  /**
   * 현재 merge를 통해 update or insert 실행
   * persist가 아닌 merge이기 때문에
   * select 쿼리를 날려 수정 대상이면 update, 저장 대상이면 insert 실행
   * -> 성능 이슈가 발생할 수 있다.
   */
  private ItemWriter<Person> jpaItemWriter() throws Exception {
    JpaItemWriter<Person> itemWriter = new JpaItemWriterBuilder<Person>()
        .entityManagerFactory(entityManagerFactory)
        .build();

    itemWriter.afterPropertiesSet();
    return itemWriter;
  }

  private ItemWriter<Person> jdbcBatchItemWriter() {
    JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriterBuilder<Person>()
        .dataSource(dataSource)
        .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
        .sql("insert into person(name, age, address) values(:name, :age, :address)")
        .build();
    itemWriter.afterPropertiesSet();
    return itemWriter;
  }

  private ItemWriter<Person> csvFileItemWriter() throws Exception {
    BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
    fieldExtractor.setNames(new String[]{"id", "name", "age", "address"});

    DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
    lineAggregator.setDelimiter(",");
    lineAggregator.setFieldExtractor(fieldExtractor);

    FlatFileItemWriter<Person> itemWriter = new FlatFileItemWriterBuilder<Person>()
        .name("csvFileItemWriter")
        .encoding("UTF-8")
        .resource(new FileSystemResource("output/test-output.csv"))
        .lineAggregator(lineAggregator)
        .headerCallback(writer -> writer.write("id,이름,나이,거주지"))
        .footerCallback(writer -> writer.write("---------------\n")) // footer에는 항상 개행문자 필요
        .append(true)
        .build();
    itemWriter.afterPropertiesSet();
    return itemWriter;
  }

  private ItemReader<Person> itemReader() {
    return new CustomItemReader<>(getItems());
  }

  private List<Person> getItems() {
    List<Person> items = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      items.add(new Person(i + 1, "test name" + i, "test age", "test address"));
    }

    return items;
  }
}
