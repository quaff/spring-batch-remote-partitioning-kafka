package com.example.batch;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaConfiguration {

	private static final String TOPIC_NAME_WORKER = "worker";

	private static final int TOPIC_PARTITION_COUNT = 3;

	@Bean
	NewTopic workerTopic() {
		return TopicBuilder.name(TOPIC_NAME_WORKER)
				.partitions(TOPIC_PARTITION_COUNT)
				.build();
	}
}
