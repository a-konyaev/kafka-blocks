package kafkablocks.examples;

import lombok.Getter;
import org.apache.kafka.clients.admin.NewTopic;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import java.util.Collections;

public class KafkaInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Getter
    private final KafkaContainerEx kafka;

    public KafkaInitializer(String topic, int numPartitions, short replicationFactor) {
        this.kafka = new KafkaContainerEx(
                Collections.singletonList(new NewTopic(topic, numPartitions, replicationFactor)));
    }

    @Override
    public void initialize(@NotNull ConfigurableApplicationContext applicationContext) {
        this.kafka.start();

        TestPropertyValues
                .of("spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers())
                .applyTo(applicationContext);

        applicationContext.addApplicationListener((ApplicationListener<ContextClosedEvent>) event -> {
            this.kafka.stop();
        });
    }
}
