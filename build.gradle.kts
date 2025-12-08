plugins {
	id("org.springframework.boot").version("4.0.1")
	id("io.spring.dependency-management").version("latest.release")
	java
}

group = "com.example.batch"
version = "0.0.1-SNAPSHOT"

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-batch-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-integration")
	implementation("org.springframework.boot:spring-boot-starter-kafka")
	implementation("org.springframework.batch:spring-batch-integration")
	implementation("org.springframework.integration:spring-integration-kafka")
	runtimeOnly("org.springframework.boot:spring-boot-docker-compose")
	runtimeOnly("com.mysql:mysql-connector-j")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.batch:spring-batch-test")
	testImplementation("org.springframework.boot:spring-boot-testcontainers")
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("org.testcontainers:testcontainers-mysql")
	testImplementation("org.testcontainers:testcontainers-kafka")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
