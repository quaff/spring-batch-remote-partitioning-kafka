package com.example.batch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
class ManagerConfiguration {

	public static final int CUSTOMER_COUNT = 789;

	public static final String CUSTOMER_FILE_LOCATION = "build/customer.txt";

	private static final int PARTITION_COUNT = 10;

	@Bean
	Job partitioningJob(JobRepository jobRepository, Step generatingFileStep, Step partitionerStep) {
		return new JobBuilder("partitioningJob", jobRepository)
				.start(generatingFileStep)
				.next(partitionerStep)
				.incrementer(new RunIdIncrementer())
				.build();
	}

	@Bean
	Step generatingFileStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
							ItemReader<Integer> generatingFileItemReader,
							ItemProcessor<Integer, Customer> generatingFileItemProcessor,
							ItemWriter<Customer> generatingFileItemWriter
	) {
		return new StepBuilder("generatingFileStep", jobRepository)
				.<Integer, Customer>chunk(10, transactionManager)
				.reader(generatingFileItemReader)
				.processor(generatingFileItemProcessor)
				.writer(generatingFileItemWriter)
				.build();
	}

	@Bean
	ItemReader<Integer> generatingFileItemReader() {
		return new IteratorItemReader<>(IntStream.rangeClosed(1, CUSTOMER_COUNT).iterator());
	}

	@Bean
	ItemProcessor<Integer, Customer> generatingFileItemProcessor() {
		return id -> new Customer(id, "name" + id);
	}

	@Bean
	ItemWriter<Customer> generatingFileItemWriter() {
		return new FlatFileItemWriterBuilder<Customer>()
				.name("generatingFileItemWriter")
				.resource(new FileSystemResource(CUSTOMER_FILE_LOCATION))
				.lineAggregator(customer -> customer.id() + "," + customer.name())
				.headerCallback(writer -> writer.write(String.valueOf(CUSTOMER_COUNT)))
				.build();
	}

	@Bean
	Step partitionerStep(RemotePartitioningManagerStepBuilderFactory stepBuilderFactory,
						 Partitioner partitioner, MessageChannel outputChannel) {
		return stepBuilderFactory.get("partitionerStep")
				.partitioner("workerStep", partitioner)
				.gridSize(PARTITION_COUNT)
				.outputChannel(outputChannel)
				.build();
	}

	@Bean
	Partitioner partitioner() {
		return (int gridSize) -> {
			int count;
			try {
				try (Stream<String> lines = Files.lines(new FileSystemResource(CUSTOMER_FILE_LOCATION).getFile().toPath(), StandardCharsets.UTF_8)) {
					count = Integer.parseInt(lines.findFirst().orElseThrow());
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			int partitionSize = count / gridSize;
			if (count % gridSize > 0) {
				partitionSize++;
			}
			Map<String, ExecutionContext> partitions = new HashMap<>();
			for (int i = 0; i < gridSize; i++) {
				ExecutionContext executionContext = new ExecutionContext();
				executionContext.put("linesToSkip", i * partitionSize + 1); // skip header
				executionContext.put("maxItemCount", partitionSize);
				partitions.put("partition" + i, executionContext);
			}
			return partitions;
		};
	}

	@Bean
	MessageChannel outputChannel() {
		return new DirectChannel();
	}

	@Bean
	IntegrationFlow outboundFlow(MessageChannel outputChannel, KafkaTemplate<?, ?> kafkaTemplate, NewTopic workerTopic) {
		KafkaProducerMessageHandler<?, ?> messageHandler = new KafkaProducerMessageHandler<>(kafkaTemplate);
		messageHandler.setTopicExpression(new LiteralExpression(workerTopic.name()));
		Function<Message<StepExecutionRequest>, Long> partitionIdFn = (m) -> {
			StepExecutionRequest executionRequest = m.getPayload();
			return executionRequest.getStepExecutionId() % workerTopic.numPartitions();
		};
		messageHandler.setPartitionIdExpression(new FunctionExpression<>(partitionIdFn));
		return IntegrationFlow
				.from(outputChannel)
				.log()
				.handle(messageHandler)
				.get();
	}
}
