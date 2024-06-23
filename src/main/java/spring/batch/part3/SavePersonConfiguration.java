package spring.batch.part3;

import jakarta.persistence.EntityManagerFactory;
import java.util.Date;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;
import spring.batch.part3.SavePersonListener.SavePersonAnnotationStepExecutionListener;
import spring.batch.part3.SavePersonListener.SavePersonStepExecutionListener;

@Configuration
@RequiredArgsConstructor
@Log4j2
public class SavePersonConfiguration {

  private final String JOB_NAME = this.getClass().getSimpleName();

  private final JobRepository jobRepository;

  private final JobLauncher jobLauncher;

  private final PlatformTransactionManager platformTransactionManager;

  private final EntityManagerFactory entityManagerFactory;

  private final PersonRepository personRepository;

  @Bean
  public void savePersonJobScheduler() {
    try {
      JobParameters jobParameters = new JobParametersBuilder()
          .addDate("time", new Date())
          .toJobParameters();
      jobLauncher.run(savePersonJob(), jobParameters);
    } catch (Exception e) {
      log.error(JOB_NAME, e);
    }
  }

  @Bean
  public Job savePersonJob() {
    final String JOB_NAME = "savePersonJob";
    return new JobBuilder(JOB_NAME, jobRepository)
        .incrementer(new RunIdIncrementer())
        .start(savePersonStep(null))
        .build();
  }

  @Bean
  @JobScope
  public Step savePersonStep(@Value("#{jobParameters[allow_duplicate]}") String allowDuplicate) {
    final String STEP_NAME = "savePersonStep";
    return new StepBuilder(STEP_NAME, jobRepository)
        .<Person, Person>chunk(10, platformTransactionManager)
        .reader(itemReader())
        .processor(itemProcessor(allowDuplicate))
        .writer(itemWriter())
        .listener(new SavePersonStepExecutionListener())
        .listener(new SavePersonAnnotationStepExecutionListener())
        .faultTolerant() // skip 예외처리 메서드 제공
        .skip(NotFoundNameException.class)
        .skipLimit(3) // 3번까지 허용
        .build();
  }

  private ItemReader<Person> itemReader() {
    DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
    DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
    lineTokenizer.setNames("name", "age", "address");
    lineMapper.setLineTokenizer(lineTokenizer);
    lineMapper.setFieldSetMapper(
        fieldSet -> new Person(fieldSet.readString(0), fieldSet.readString(1),
            fieldSet.readString(2)));

    FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
        .name("savePersonItemReader")
        .encoding("UTF-8")
        .linesToSkip(1)
        .resource(new ClassPathResource("person.csv"))
        .lineMapper(lineMapper)
        .build();

    return itemReader;
  }

  private ItemProcessor<Person, Person> itemProcessor(String allowDuplicate) {
    DuplicateValidationProcessor<Person> duplicateValidationProcessor = new DuplicateValidationProcessor<>(
        Person::getName, Boolean.parseBoolean(allowDuplicate));

    ItemProcessor<Person, Person> validationProcessor = item -> {
      if (item.isNotEmptyName()) {
        return item;
      }
      throw new NotFoundNameException();
    };

    CompositeItemProcessor<Person, Person> itemProcessor = new CompositeItemProcessorBuilder<Person, Person>()
        .delegates(validationProcessor, duplicateValidationProcessor)
        .build();

    return itemProcessor;
  }

  private ItemWriter<Person> itemWriter() {
    return new RepositoryItemWriterBuilder<Person>()
        .repository(personRepository)
        .build();
  }
}
