plugins {
	id("org.springframework.boot").version("latest.release")
	id("io.spring.dependency-management").version("latest.release")
	java
}

group = "com.example.batch"
version = "0.0.1-SNAPSHOT"

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-batch")
	implementation("org.springframework.batch:spring-batch-integration")
	implementation("org.springframework.boot:spring-boot-starter-integration")
	implementation("org.springframework.integration:spring-integration-kafka")
	implementation("com.fasterxml.jackson.core:jackson-databind")
	runtimeOnly("org.springframework.boot:spring-boot-docker-compose")
	runtimeOnly("com.mysql:mysql-connector-j")
}

tasks.withType<Test>() {
	useJUnitPlatform()
}
