package com.example.batch;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

@Configuration
class ManagerConfiguration {

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

	@Bean
	Job partitioningJob(JobRepository jobRepository, Step partitionerStep) {
		return new JobBuilder("partitioningJob", jobRepository)
				.start(partitionerStep)
				.incrementer(new RunIdIncrementer())
				.build();
	}

	@Bean
	Partitioner partitioner() {
		return (int gridSize) -> {
			Map<String, ExecutionContext> partitions = new HashMap<>();
			for (int i = 0; i < gridSize; i++) {
				ExecutionContext executionContext = new ExecutionContext();
				int range = 100;
				executionContext.put("id.min", i * range + 1);
				executionContext.put("id.max", (i + 1) * range);
				partitions.put("partition" + i, executionContext);
			}
			return partitions;
		};
	}

	@Bean
	Step partitionerStep(RemotePartitioningManagerStepBuilderFactory stepBuilderFactory,
						 Partitioner partitioner, MessageChannel outputChannel) {
		return stepBuilderFactory.get("partitionerStep")
				.partitioner("workerStep", partitioner)
				.gridSize(10)
				.outputChannel(outputChannel)
				.build();
	}

}
