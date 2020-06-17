package kafkablocks.examples.prioritizer.producer;

import kafkablocks.examples.prioritizer.Constants;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ProducerApp {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApp.class, args);
    }

    @Bean
    public NewTopic lowPriorityTopic() {
        return new NewTopic(Constants.LOW_PRIORITY_TOPIC, 3, (short) 1);
    }

    @Bean
    public NewTopic highPriorityTopic() {
        return new NewTopic(Constants.LOW_PRIORITY_TOPIC, 3, (short) 1);
    }
}
