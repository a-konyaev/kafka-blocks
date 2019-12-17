package kafkablocks.examples;

import kafkablocks.processing.EnableEventProcessorRunner;
import kafkablocks.publisher.KafkaPublisherImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@EnableEventProcessorRunner
@Import(KafkaPublisherImpl.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
