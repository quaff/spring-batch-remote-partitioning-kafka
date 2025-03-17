package com.example.batch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
class JobConfiguration {

	public static final int CHUNK_SIZE = 10;

	public static final int CUSTOMER_COUNT = 789;

	public static final String CUSTOMER_FILE_LOCATION = "build/customer.txt";

	public static final int USER_COUNT = 1000;

	public static final String USER_FILE_LOCATION = "build/user.txt";

	private static final int PARTITION_COUNT = 10;

	@Bean
	Job demoJob(TaskExecutor taskExecutor, JobRepository jobRepository,
				Step generatingUserFileStep, Step importUserStep,
				Step generatingCustomerFileStep, Step importCustomerManagerStep) {
		Flow userFlow = new FlowBuilder<Flow>("userFlow").from(generatingUserFileStep).next(importUserStep).end();
		Flow customerFlow = new FlowBuilder<Flow>("customerFlow").from(generatingCustomerFileStep).next(importCustomerManagerStep).end();
		Flow splitFlow = new FlowBuilder<Flow>("splitFlow").split(taskExecutor).add(userFlow, customerFlow).build();
		return new JobBuilder("demoJob", jobRepository)
				.start(splitFlow)
				.end()
				.incrementer(new RunIdIncrementer())
				.build();
	}

	@Bean
	Step generatingUserFileStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
								ItemReader<User> generatingUserFileItemReader, ItemWriter<User> generatingUserFileItemWriter) {
		return new StepBuilder("generatingUserFileStep", jobRepository)
				.<User, User>chunk(CHUNK_SIZE, transactionManager)
				.reader(generatingUserFileItemReader)
				.writer(generatingUserFileItemWriter)
				.build();
	}

	@Bean
	ItemReader<User> generatingUserFileItemReader() {
		return new IteratorItemReader<>(IntStream.rangeClosed(1, USER_COUNT).mapToObj(i -> new User(i, "user" + i)).iterator());
	}

	@Bean
	ItemWriter<User> generatingUserFileItemWriter() {
		return new FlatFileItemWriterBuilder<User>()
				.name("generatingUserFileItemWriter")
				.resource(new FileSystemResource(USER_FILE_LOCATION))
				.lineAggregator(user -> user.id() + "," + user.name())
				.build();
	}

	@Bean
	Step importUserStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
						ItemReader<User> importUserItemReader, ItemWriter<User> importUserItemWriter) {
		return new StepBuilder("importUserStep", jobRepository)
				.<User, User>chunk(CHUNK_SIZE, transactionManager)
				.reader(importUserItemReader)
				.writer(importUserItemWriter)
				.build();
	}

	@Bean
	FlatFileItemReader<User> importUserItemReader() {
		return new FlatFileItemReaderBuilder<User>()
				.name("importUserItemReader")
				.resource(new FileSystemResource(USER_FILE_LOCATION))
				.delimited()
				.names("id", "name")
				.targetType(User.class)
				.build();
	}

	@Bean
	ItemWriter<User> importUserItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<User>()
				.beanMapped()
				.dataSource(dataSource)
				.sql("insert into user (id,name) values (:id,:name)")
				.build();
	}

	@Bean
	Step generatingCustomerFileStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
									ItemReader<Customer> generatingCustomerFileItemReader,
									ItemWriter<Customer> generatingCustomerFileItemWriter) {
		return new StepBuilder("generatingCustomerFileStep", jobRepository)
				.<Customer, Customer>chunk(CHUNK_SIZE, transactionManager)
				.reader(generatingCustomerFileItemReader)
				.writer(generatingCustomerFileItemWriter)
				.build();
	}

	@Bean
	ItemReader<Customer> generatingCustomerFileItemReader() {
		return new IteratorItemReader<>(IntStream.rangeClosed(1, CUSTOMER_COUNT).mapToObj(i -> new Customer(i, "customer" + i)).iterator());
	}

	@Bean
	ItemWriter<Customer> generatingCustomerFileItemWriter() {
		return new FlatFileItemWriterBuilder<Customer>()
				.name("generatingCustomerFileItemWriter")
				.resource(new FileSystemResource(CUSTOMER_FILE_LOCATION))
				.lineAggregator(customer -> customer.id() + "," + customer.name())
				.headerCallback(writer -> writer.write(String.valueOf(CUSTOMER_COUNT)))
				.build();
	}

	@Bean
	Step importCustomerManagerStep(RemotePartitioningManagerStepBuilderFactory stepBuilderFactory,
								   Partitioner importCustomerPartitioner, JdbcTemplate jdbcTemplate,
								   MessageChannel outputChannel) {
		return stepBuilderFactory.get("importCustomerManagerStep")
				.partitioner("importCustomerWorkerStep", importCustomerPartitioner)
				.listener(new StepExecutionListener() {
					@Override
					public ExitStatus afterStep(StepExecution stepExecution) {
						Integer expectedRows = parseFirstLineAsInteger(new FileSystemResource(CUSTOMER_FILE_LOCATION));
						Integer actualRows = jdbcTemplate.queryForObject("select count(*) from customer", int.class);
						if (!Objects.equals(expectedRows, actualRows)) {
							return ExitStatus.FAILED.addExitDescription("Expected rows is %d but actual rows is %d".formatted(expectedRows, actualRows));
						}
						return null;
					}
				})
				.gridSize(PARTITION_COUNT)
				.outputChannel(outputChannel)
				.build();
	}

	@StepScope
	@Bean
	Partitioner importCustomerPartitioner() {
		return (int gridSize) -> {
			int count = parseFirstLineAsInteger(new FileSystemResource(CUSTOMER_FILE_LOCATION));
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

	private static int parseFirstLineAsInteger(Resource resource) {
		try {
			try (Stream<String> lines = Files.lines(resource.getFile().toPath(), StandardCharsets.UTF_8)) {
				return Integer.parseInt(lines.findFirst().orElseThrow());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
