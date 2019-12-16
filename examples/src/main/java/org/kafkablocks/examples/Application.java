package org.kafkablocks.examples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.kafkablocks.EventTopicProperties;
import org.kafkablocks.processing.EnableEventProcessorRunner;

@SpringBootApplication
@EnableEventProcessorRunner
@Import(EventTopicProperties.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
