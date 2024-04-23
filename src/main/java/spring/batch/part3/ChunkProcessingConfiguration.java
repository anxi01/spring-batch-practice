package spring.batch.part3;

import io.micrometer.core.instrument.util.StringUtils;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ChunkProcessingConfiguration {

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;

  public ChunkProcessingConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean
  public Job chunkProcessingJob() {
    return jobBuilderFactory.get("chunkProcessingJob")
        .incrementer(new RunIdIncrementer())
        .start(this.taskBaseStep())
        .next(this.chunkBaseStep())
        .build();
  }

  @Bean
  public Step chunkBaseStep() {
    return stepBuilderFactory.get("chunkBaseStep")
        .<String, String>chunk(10) // 100개의 data를 10개씩 나눠서 실행 (총 10회 실행)
        .reader(itemReader())
        .processor(itemProcessor())
        .writer(itemWriter())
        .build();
  }

  private ItemReader<String> itemReader() {
    return new ListItemReader<>(getItems());
  }

  private ItemProcessor<String, String> itemProcessor() {
    return item -> item + ", Spring Batch";
  }

  private ItemWriter<String> itemWriter() {
    return items-> log.info("chunk item size : {}", items.size());
  }

  @Bean
  public Step taskBaseStep() {
    return stepBuilderFactory.get("taskBaseStep")
        .tasklet(this.tasklet())
        .build();
  }

//  private Tasklet tasklet() {
//    return ((contribution, chunkContext) -> {
//      List<String> items = getItems();
//      log.info("task item size : {}", items.size());
//      return RepeatStatus.FINISHED;
//    });
//  }

  // Tasklet으로 Chunk 기능 수행하는 메서드
  private Tasklet tasklet() {
    List<String> items = getItems();

    return ((contribution, chunkContext) -> {
      StepExecution stepExecution = contribution.getStepExecution();
      JobParameters jobParameters = stepExecution.getJobParameters();

      String value = jobParameters.getString("chunkSize", "10");
      int chunkSize = StringUtils.isNotEmpty(value) ? Integer.parseInt(value) : 10;

      int fromIndex = stepExecution.getReadCount();
      int toIndex = fromIndex + chunkSize;

      if (fromIndex >= items.size()) {
        return RepeatStatus.FINISHED;
      }

      List<String> subList = items.subList(fromIndex, toIndex);

      log.info("task item size : {}", subList.size());

      stepExecution.setReadCount(toIndex);

      return RepeatStatus.CONTINUABLE;
    });
  }

  private List<String> getItems() {
    List<String> items = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      items.add(i + " Hello");
    }
    return items;
  }
}
