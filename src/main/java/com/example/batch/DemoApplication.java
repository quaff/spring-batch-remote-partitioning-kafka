package com.example.batch;

import jakarta.annotation.PostConstruct;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@EnableBatchIntegration
@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Autowired
	DataSource dataSource;

	@Value("classpath:org/springframework/batch/core/schema-mysql.sql")
	Resource schemaResource;

	@PostConstruct
	void createTables() throws Exception {
		try (Connection conn = dataSource.getConnection()) {
			try (ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), conn.getSchema(), "BATCH_JOB_INSTANCE", new String[]{"TABLE"})) {
				if (rs.next()) {
					return;
				}
			}

			try (Statement stmt = conn.createStatement()) {
				String content = schemaResource.getContentAsString(StandardCharsets.UTF_8);
				for (String ddl : content.split(";")) {
					if (!ddl.isBlank()) {
						stmt.execute(ddl);
					}
				}
				stmt.execute("create table customer(id integer)");
			}
		}
	}
}
