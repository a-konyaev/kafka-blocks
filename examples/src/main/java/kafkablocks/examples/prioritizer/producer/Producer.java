package kafkablocks.examples.prioritizer.producer;

import kafkablocks.examples.prioritizer.Constants;
import kafkablocks.utils.RandomUtils;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class Producer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    @PostConstruct
    private void init() {
        executor.execute(this::produceLowPriorityMessages);
        executor.execute(this::produceHighPriorityMessages);
    }

    @PreDestroy
    private void shutdown() {
        executor.shutdown();
    }

    @SneakyThrows
    private void produceLowPriorityMessages() {
        int i = 0;
        while (!Thread.currentThread().isInterrupted()) {
            var msg = String.format("low-%06d", i++);
            kafkaTemplate.send(Constants.LOW_PRIORITY_TOPIC, UUID.randomUUID().toString(), msg);
            log.info("sent low: {}", msg);
            Thread.sleep(200);
        }
    }

    @SneakyThrows
    private void produceHighPriorityMessages() {
        int i = 0;
        while (!Thread.currentThread().isInterrupted()) {
            var msg = String.format("high-%06d", i++);
            kafkaTemplate.send(Constants.HIGH_PRIORITY_TOPIC, UUID.randomUUID().toString(), msg);
            log.info("sent high: {}", msg);

            if (i % 5 == 0) {
                Thread.sleep(RandomUtils.getRandomInt(1_000, 5_000));
            } else {
                Thread.sleep(RandomUtils.getRandomInt(50, 150));
            }
        }
    }

}
