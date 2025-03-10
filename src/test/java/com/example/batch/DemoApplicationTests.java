package com.example.batch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ImportTestcontainers
class DemoApplicationTests {

	@Container
	@ServiceConnection
	static MySQLContainer<?> database = new MySQLContainer<>("mysql");

	@Container
	@ServiceConnection
	static ConfluentKafkaContainer container = new ConfluentKafkaContainer(
			DockerImageName.parse("confluentinc/cp-kafka"));

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	void test() {
		assertThat(jdbcTemplate.queryForObject("select count(*) from customer", int.class)).isEqualTo(1000);
	}
} 