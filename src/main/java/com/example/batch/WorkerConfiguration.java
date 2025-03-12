package com.example.batch;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.IntStream;
import javax.sql.DataSource;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
class WorkerConfiguration {

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

	@Bean
	Step workerStep(RemotePartitioningWorkerStepBuilderFactory stepBuilderFactory,
					PlatformTransactionManager transactionManager, MessageChannel inputChannel,
					ItemReader<Integer> itemReader, ItemProcessor<Integer, Customer> itemProcessor,
					ItemWriter<Customer> itemWriter) {
		return stepBuilderFactory.get("workerStep")
				.inputChannel(inputChannel)
				.<Integer, Customer>chunk(10, transactionManager)
				.reader(itemReader)
				.processor(itemProcessor)
				.writer(itemWriter)
				.build();
	}

	@Bean
	ItemWriter<Customer> itemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Customer>()
				.beanMapped()
				.dataSource(dataSource)
				.sql("insert into customer (id) values (:id)")
				.build();
	}

	@Bean
	ItemProcessor<Integer, Customer> itemProcessor() {
		return item -> {
			try {
				Thread.sleep(50); // simulation
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return new Customer(item);
		};
	}

	@Bean
	@StepScope
	ItemReader<Integer> itemReader(@Value("#{stepExecutionContext['id.min']}") int min, @Value("#{stepExecutionContext['id.max']}") int max) {
		return new IteratorItemReader<>(IntStream.rangeClosed(min, max).iterator());
	}

	record Customer(Integer id) {

	}

	@Autowired
	DataSource dataSource;

	@PostConstruct
	void createTables() throws Exception {
		try (Connection conn = dataSource.getConnection()) {
			try (ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), conn.getSchema(), "customer", new String[]{"TABLE"})) {
				if (rs.next()) {
					return;
				}
			}
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("create table customer(id integer)");
			}
		}
	}
}
