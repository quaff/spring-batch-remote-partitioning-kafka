package com.example.batch;


import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.time.LocalDate;
import java.util.Map;

import static com.example.batch.JobConfiguration.*;

@EnableBatchIntegration
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) throws Exception {

        JobParameter<LocalDate> date = new JobParameter<>(LocalDate.now(), LocalDate.class, true);
        JobParameter<Long> userCount = new JobParameter<>(1000L, Long.class, false);
        JobParameter<Long> customerCount = new JobParameter<>(789L, Long.class, false);
        JobParameters jobParameters = new JobParameters(Map.of(JOB_PARAMETER_DATE, date, JOB_PARAMETER_USER_COUNT, userCount, JOB_PARAMETER_CUSTOMER_COUNT, customerCount));

        String[] argsToUse = jobParameters.getParameters().entrySet().stream().map(
                entry ->
                        String.format("%s=%s,%s,%s", entry.getKey(), entry.getValue().getValue(), entry.getValue().getType().getName(), entry.getValue().isIdentifying())
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
