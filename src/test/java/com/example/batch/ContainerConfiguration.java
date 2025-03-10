package com.example.batch;

import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@ImportTestcontainers
class ContainerConfiguration {

	@Container
	@ServiceConnection
	static MySQLContainer<?> database = new MySQLContainer<>("mysql");

	@Container
	@ServiceConnection
	static ConfluentKafkaContainer container = new ConfluentKafkaContainer(
			DockerImageName.parse("confluentinc/cp-kafka"));

}
