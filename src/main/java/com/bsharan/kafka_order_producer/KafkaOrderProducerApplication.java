package com.bsharan.kafka_order_producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaOrderProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaOrderProducerApplication.class, args);
	}

}

// start the application -> % ./mvnw spring-boot:run
// start the application at a given port -> % ./mvnw spring-boot:run -Dspring-boot.run.arguments="--server.port=8081"
