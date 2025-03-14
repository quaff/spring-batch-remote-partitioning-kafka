package com.example.batch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(ContainerConfiguration.class)
class DemoApplicationTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	void test() {
		assertThat(jdbcTemplate.queryForObject("select count(*) from user", int.class)).isEqualTo(JobConfiguration.USER_COUNT);
		assertThat(jdbcTemplate.queryForObject("select count(*) from customer", int.class)).isEqualTo(JobConfiguration.CUSTOMER_COUNT);
	}
} 