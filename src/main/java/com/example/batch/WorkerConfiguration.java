package com.example.batch;

import java.time.LocalDate;
import javax.sql.DataSource;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

import static com.example.batch.JobConfiguration.CHUNK_SIZE;
import static com.example.batch.JobConfiguration.CUSTOMER_FILE_LOCATION;
import static com.example.batch.JobConfiguration.JOB_PARAMETER_DATE;

@Configuration
class WorkerConfiguration {

	@Bean
	Step importCustomerWorkerStep(RemotePartitioningWorkerStepBuilderFactory stepBuilderFactory,
								  PlatformTransactionManager transactionManager, MessageChannel inputChannel,
								  ItemReader<Customer> importCustomerItemReader, ItemProcessor<Customer, Customer> importCustomerItemProcessor,
								  ItemWriter<Customer> importCustomerItemWriter) {
		return stepBuilderFactory.get("importCustomerWorkerStep")
				.inputChannel(inputChannel)
				.<Customer, Customer>chunk(CHUNK_SIZE, transactionManager)
				.reader(importCustomerItemReader)
				.processor(importCustomerItemProcessor)
				.writer(importCustomerItemWriter)
				.build();
	}

	@Bean
	@StepScope
	FlatFileItemReader<Customer> importCustomerItemReader(@Value("#{jobParameters['" + JOB_PARAMETER_DATE + "']}") LocalDate date,
														  @Value("#{stepExecutionContext['linesToSkip']}") int linesToSkip,
														  @Value("#{stepExecutionContext['maxItemCount']}") int maxItemCount) {
		return new FlatFileItemReaderBuilder<Customer>()
				.name("importCustomerItemReader")
				.resource(new FileSystemResource(CUSTOMER_FILE_LOCATION + "." + date))
				.delimited()
				.names("id", "name")
				.targetType(Customer.class)
				.linesToSkip(linesToSkip)
				.maxItemCount(maxItemCount)
				.build();
	}

	@Bean
	ItemProcessor<Customer, Customer> importCustomerItemProcessor() {
		return item -> {
			try {
				Thread.sleep(50); // simulation
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return item;
		};
	}

	@Bean
	JdbcBatchItemWriter<Customer> importCustomerItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Customer>()
				.beanMapped()
				.dataSource(dataSource)
				.sql("insert into customer (id,name) values (:id,:name)")
				.build();
	}

	@Bean
	MessageChannel inputChannel(TaskExecutor taskExecutor) {
		// return new DirectChannel(); // will execute steps sequentially if concurrency of kafka listener is 1
		// return new QueueChannel(); // will execute steps sequentially if taskExecutor of PollerMetadata not set
		return new ExecutorChannel(taskExecutor);
	}

	@Bean
	IntegrationFlow inboundFlow(MessageChannel inputChannel, ConsumerFactory<String, String> consumerFactory, NewTopic workerTopic) {
		return IntegrationFlow
				.from(Kafka.messageDrivenChannelAdapter(consumerFactory, workerTopic.name()))
				.channel(inputChannel)
				.get();
	}
}
