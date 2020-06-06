package kafkablocks.examples;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.testcontainers.containers.KafkaContainer;

public class KafkaInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Getter
    private final KafkaContainer kafka;

    public KafkaInitializer() {
        this.kafka = new KafkaContainer();
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
