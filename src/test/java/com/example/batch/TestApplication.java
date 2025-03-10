package com.example.batch;

import org.springframework.boot.SpringApplication;

public class TestApplication {

	public static void main(String[] args) {
		String[] argsToUse = new String[args.length + 1];
		argsToUse[0] = "--spring.docker.compose.enabled=false";
		if (args.length > 0) {
			System.arraycopy(args, 0, argsToUse, 1, args.length);
		}
		SpringApplication
				.from(DemoApplication::main)
				.with(ContainerConfiguration.class)
				.run(argsToUse);
	}
}