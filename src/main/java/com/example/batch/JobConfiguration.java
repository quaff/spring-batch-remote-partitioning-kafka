package com.example.batch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
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

	public static final String JOB_PARAMETER_DATE = "date";

	public static final String JOB_PARAMETER_USER_COUNT = "user.count";

	public static final String JOB_PARAMETER_CUSTOMER_COUNT = "customer.count";

	public static final int CHUNK_SIZE = 10;

	public static final int PARTITION_COUNT = 10;

	public static final String USER_FILE_LOCATION = "build/user.txt";

	public static final String CUSTOMER_FILE_LOCATION = "build/customer.txt";

	@Bean
	Job demoJob(TaskExecutor applicationTaskExecutor, JobRepository jobRepository,
				Step generatingUserFileStep, Step importUserStep,
				Step generatingCustomerFileStep, Step importCustomerManagerStep,
				Step verifyStep, JobParametersValidator validator) {
		Flow userFlow = new FlowBuilder<Flow>("userFlow").from(generatingUserFileStep).next(importUserStep).end();
		Flow customerFlow = new FlowBuilder<Flow>("customerFlow").from(generatingCustomerFileStep).next(importCustomerManagerStep).end();
		Flow splitFlow = new FlowBuilder<Flow>("splitFlow").split(applicationTaskExecutor).add(userFlow, customerFlow).build();
		return new JobBuilder("demoJob", jobRepository)
				.start(splitFlow)
				.next(verifyStep)
				.end()
				.validator(validator)
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
	@StepScope
	IteratorItemReader<User> generatingUserFileItemReader(@Value("#{jobParameters['" + JOB_PARAMETER_USER_COUNT + "']}") int count) {
		return new IteratorItemReader<>(IntStream.rangeClosed(1, count).mapToObj(i -> new User(i, "user" + i)).iterator());
	}

	@Bean
	@StepScope
	FlatFileItemWriter<User> generatingUserFileItemWriter(@Value("#{jobParameters['" + JOB_PARAMETER_DATE + "']}") LocalDate date) {
		return new FlatFileItemWriterBuilder<User>()
				.name("generatingUserFileItemWriter")
				.resource(new FileSystemResource(USER_FILE_LOCATION + '.' + date))
				.delimited()
				.names("id", "name")
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
	@StepScope
	FlatFileItemReader<User> importUserItemReader(@Value("#{jobParameters['" + JOB_PARAMETER_DATE + "']}") LocalDate date) {
		return new FlatFileItemReaderBuilder<User>()
				.name("importUserItemReader")
				.resource(new FileSystemResource(USER_FILE_LOCATION + '.' + date))
				.delimited()
				.names("id", "name")
				.targetType(User.class)
				.build();
	}

	@Bean
	JdbcBatchItemWriter<User> importUserItemWriter(DataSource dataSource) {
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
	@StepScope
	IteratorItemReader<Customer> generatingCustomerFileItemReader(@Value("#{jobParameters['" + JOB_PARAMETER_CUSTOMER_COUNT + "']}") int count) {
		return new IteratorItemReader<>(IntStream.rangeClosed(1, count).mapToObj(i -> new Customer(i, "customer" + i)).iterator());
	}

	@Bean
	@StepScope
	FlatFileItemWriter<Customer> generatingCustomerFileItemWriter(@Value("#{jobParameters['" + JOB_PARAMETER_DATE + "']}") LocalDate date,
														  @Value("#{jobParameters['" + JOB_PARAMETER_CUSTOMER_COUNT + "']}") int count) {
		return new FlatFileItemWriterBuilder<Customer>()
				.name("generatingCustomerFileItemWriter")
				.resource(new FileSystemResource(CUSTOMER_FILE_LOCATION + '.' + date))
				.delimited()
				.names("id", "name")
				.headerCallback(writer -> writer.write(String.valueOf(count)))
				.build();
	}

	@Bean
	Step importCustomerManagerStep(
								   RemotePartitioningManagerStepBuilderFactory stepBuilderFactory,
								   Partitioner importCustomerPartitioner, JdbcTemplate jdbcTemplate,
								   MessageChannel outputChannel) {
		return stepBuilderFactory.get("importCustomerManagerStep")
				.partitioner("importCustomerWorkerStep", importCustomerPartitioner)
				.listener(new StepExecutionListener() {
					@Override
					public ExitStatus afterStep(StepExecution stepExecution) {
						// for demo, It's covered by verifyStep
						Long expectedRows = stepExecution.getJobParameters().getLong(JOB_PARAMETER_CUSTOMER_COUNT);
						Long actualRows = jdbcTemplate.queryForObject("select count(*) from customer", Long.class);
						if (!Objects.equals(expectedRows, actualRows)) {
							return ExitStatus.FAILED.addExitDescription("Expected customer rows is %d but actual rows is %d".formatted(expectedRows, actualRows));
						}
						return null;
					}
				})
				.gridSize(PARTITION_COUNT)
				.outputChannel(outputChannel)
				.build();
	}

	@Bean
	@StepScope
	Partitioner importCustomerPartitioner(@Value("#{jobParameters['" + JOB_PARAMETER_DATE + "']}") LocalDate date) {
		return (int gridSize) -> {
			int count = parseFirstLineAsInteger(new FileSystemResource(CUSTOMER_FILE_LOCATION + '.' + date));
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
	Step verifyStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcTemplate jdbcTemplate) {
		return new StepBuilder("verifyStep", jobRepository).tasklet((contribution, chunkContext) -> {
					JobParameters parameters = contribution.getStepExecution().getJobParameters();
					Long expectedRows = parameters.getLong(JOB_PARAMETER_USER_COUNT);
					Long actualRows = jdbcTemplate.queryForObject("select count(*) from user", Long.class);
					if (!Objects.equals(expectedRows, actualRows)) {
						throw new RuntimeException("Expected user rows is %d but actual rows is %d".formatted(expectedRows, actualRows));
					}
					expectedRows = parameters.getLong(JOB_PARAMETER_CUSTOMER_COUNT);
					actualRows = jdbcTemplate.queryForObject("select count(*) from customer", Long.class);
					if (!Objects.equals(expectedRows, actualRows)) {
						throw new RuntimeException("Expected customer rows is %d but actual rows is %d".formatted(expectedRows, actualRows));
					}
					return RepeatStatus.FINISHED;
				},transactionManager)
				.build();
	}

	@Bean
	JobParametersValidator validator() {
		return parameters -> {
			JobParameter<?> dateParameter = parameters.getParameter(JOB_PARAMETER_DATE);
			if (dateParameter == null) {
				throw new JobParametersInvalidException("Missing identifying job parameter \"" + JOB_PARAMETER_DATE + "\"");
			} else if (!dateParameter.isIdentifying()) {
				throw new JobParametersInvalidException("Job parameter \"" + JOB_PARAMETER_DATE + "\" should be identifying");
			} else if (dateParameter.getType() != LocalDate.class) {
				throw new JobParametersInvalidException("Job parameter \"" + JOB_PARAMETER_DATE + "\" should be an LocalDate");
			}

			JobParameter<?> userCountParameter = parameters.getParameter(JOB_PARAMETER_USER_COUNT);
			if (userCountParameter == null) {
				throw new JobParametersInvalidException("Missing job parameter \"" + JOB_PARAMETER_USER_COUNT + "\"");
			} else if (userCountParameter.isIdentifying()) {
				throw new JobParametersInvalidException("Job parameter \"" + JOB_PARAMETER_USER_COUNT + "\" should not be identifying");
			} else if (userCountParameter.getType() != Long.class) {
				throw new JobParametersInvalidException("Job parameter \"" + JOB_PARAMETER_USER_COUNT + "\" should be an Long");
			}

			JobParameter<?> customerCountParameter = parameters.getParameter(JOB_PARAMETER_CUSTOMER_COUNT);
			if (customerCountParameter == null) {
				throw new JobParametersInvalidException("Missing job parameter \"" + JOB_PARAMETER_CUSTOMER_COUNT + "\"");
			} else if (customerCountParameter.isIdentifying()) {
				throw new JobParametersInvalidException("Job parameter \"" + JOB_PARAMETER_CUSTOMER_COUNT + "\" should not be identifying");
			} else if (customerCountParameter.getType() != Long.class) {
				throw new JobParametersInvalidException("Job parameter \"" + JOB_PARAMETER_CUSTOMER_COUNT + "\" should be an Long");
			}
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
