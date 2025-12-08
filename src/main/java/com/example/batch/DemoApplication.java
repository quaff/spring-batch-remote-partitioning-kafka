package com.example.batch;


import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameter;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.time.LocalDate;
import java.util.Set;

import static com.example.batch.JobConfiguration.*;

@EnableBatchIntegration
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) throws Exception {

        JobParameter<LocalDate> date = new JobParameter<>(JOB_PARAMETER_DATE, LocalDate.now(), LocalDate.class, true);
        JobParameter<Long> userCount = new JobParameter<>(JOB_PARAMETER_USER_COUNT, 1000L, Long.class, false);
        JobParameter<Long> customerCount = new JobParameter<>(JOB_PARAMETER_CUSTOMER_COUNT, 789L, Long.class, false);
        JobParameters jobParameters = new JobParameters(Set.of(date, userCount, customerCount));

        String[] argsToUse = jobParameters.parameters().stream().map(
                entry ->
                        String.format("%s=%s,%s,%s", entry.name(), entry.value(), entry.type().getName(), entry.identifying())
        ).toArray(String[]::new);
        if (args.length > 0) {
            String[] mergedArgs = new String[argsToUse.length + args.length];
            System.arraycopy(argsToUse, 0, mergedArgs, 0, argsToUse.length);
            System.arraycopy(args, 0, mergedArgs, argsToUse.length, args.length);
            argsToUse = mergedArgs;
        }
        ApplicationContext context = SpringApplication.run(DemoApplication.class, argsToUse);
        JobRepository jobRepository = context.getBean(JobRepository.class);
        while (true) {
            JobExecution jobExecution = jobRepository.getLastJobExecution(JOB_NAME_DEMO, jobParameters);
            if (jobExecution == null || jobExecution.isRunning()) {
                Thread.sleep(5000L);
                continue;
            }
            System.exit(jobExecution.getStatus() == BatchStatus.COMPLETED ? 0 : 1);
        }
    }

}
