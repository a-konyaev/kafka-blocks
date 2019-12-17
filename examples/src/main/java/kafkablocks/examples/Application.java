package kafkablocks.examples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import kafkablocks.EventTopicProperties;
import kafkablocks.processing.EnableEventProcessorRunner;

@SpringBootApplication
@EnableEventProcessorRunner
@Import(EventTopicProperties.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
