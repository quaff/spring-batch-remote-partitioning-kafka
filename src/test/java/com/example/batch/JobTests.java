package com.example.batch;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.example.batch.JobConfiguration.JOB_PARAMETER_CUSTOMER_COUNT;
import static com.example.batch.JobConfiguration.JOB_PARAMETER_DATE;
import static com.example.batch.JobConfiguration.JOB_PARAMETER_USER_COUNT;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBatchTest
@SpringBootTest(properties = "spring.batch.job.enabled=false")
@Import(ContainerConfiguration.class)
class JobTests {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	public void test() throws Exception {
		long userCount = 1000;
		long customerCount = 789;
		JobExecution jobExecution = jobLauncherTestUtils.launchJob(
				new JobParametersBuilder()
						.addJobParameter(JOB_PARAMETER_DATE, new JobParameter<>(LocalDate.now(), LocalDate.class))
						.addJobParameter(JOB_PARAMETER_USER_COUNT, new JobParameter<>(userCount, Long.class, false))
						.addJobParameter(JOB_PARAMETER_CUSTOMER_COUNT, new JobParameter<>(customerCount, Long.class, false))
						.toJobParameters()
		);

		assertThat(jobExecution.getExitStatus().getExitCode()).isEqualTo("COMPLETED");
		assertThat(jdbcTemplate.queryForObject("select count(*) from user", int.class)).isEqualTo(userCount);
		assertThat(jdbcTemplate.queryForObject("select count(*) from customer", int.class)).isEqualTo(customerCount);
	}
} 