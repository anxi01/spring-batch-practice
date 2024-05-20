package spring.batch.part3;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import spring.batch.TestConfiguration;

@SpringBatchTest // JobScope 동작하게 함.
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SavePersonConfiguration.class, TestConfiguration.class})
public class SavePersonConfigurationTest {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private PersonRepository personRepository;

  @AfterEach
  public void tearDown() throws Exception {
    personRepository.deleteAll();
  }

  @Test
  public void test_not_allow_duplicate() throws Exception {
    // given (테스트에 필요한 데이터 생성)
    JobParameters jobParameters = new JobParametersBuilder()
        .addString("allow_duplicate", "false")
        .toJobParameters();

    // when (테스트 대상 실행)
    JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

    // then (데이터 검증)
    Assertions.assertThat(jobExecution.getStepExecutions().stream()
            .mapToInt(StepExecution::getWriteCount)
            .sum())
        .isEqualTo(personRepository.count())
        .isEqualTo(3);
  }

  @Test
  public void test_allow_duplicate() throws Exception {
    // given (테스트에 필요한 데이터 생성)
    JobParameters jobParameters = new JobParametersBuilder()
        .addString("allow_duplicate", "true")
        .toJobParameters();

    // when (테스트 대상 실행)
    JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

    // then (데이터 검증)
    Assertions.assertThat(jobExecution.getStepExecutions().stream()
            .mapToInt(StepExecution::getWriteCount)
            .sum())
        .isEqualTo(personRepository.count())
        .isEqualTo(100);
  }

  @Test
  void test_step() {
    // JobParameter가 없으면 Boolean.parseBoolean은 false를 동작
    // 따라서 중복 허용되지 않고 예측 값은 3이 된다.
    JobExecution jobExecution = jobLauncherTestUtils.launchStep("savePersonStep");

    Assertions.assertThat(jobExecution.getStepExecutions().stream()
            .mapToInt(StepExecution::getWriteCount)
            .sum())
        .isEqualTo(personRepository.count())
        .isEqualTo(3);
  }
}
