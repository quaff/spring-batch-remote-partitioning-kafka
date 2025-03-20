package com.example.batch;

import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDate;

import static com.example.batch.JobConfiguration.JOB_PARAMETER_CUSTOMER_COUNT;
import static com.example.batch.JobConfiguration.JOB_PARAMETER_DATE;
import static com.example.batch.JobConfiguration.JOB_PARAMETER_USER_COUNT;

@EnableBatchIntegration
@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		String[] argsToUse = new String[args.length + 3];
		argsToUse[0] = JOB_PARAMETER_DATE + "=" + LocalDate.now() + "," + LocalDate.class.getName() + ",true";
		argsToUse[1] = JOB_PARAMETER_USER_COUNT + "=1000,java.lang.Long,false";
		argsToUse[2] = JOB_PARAMETER_CUSTOMER_COUNT + "=789,java.lang.Long,false";
		if (args.length > 0) {
			System.arraycopy(args, 0, argsToUse, 3, args.length);
		}
		SpringApplication.run(DemoApplication.class, argsToUse);
	}

}
