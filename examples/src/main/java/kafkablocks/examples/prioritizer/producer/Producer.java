package kafkablocks.examples.prioritizer.producer;

import kafkablocks.concurrent.WaitHandle;
import kafkablocks.examples.prioritizer.Constants;
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
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class Producer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final WaitHandle stop = new WaitHandle();

    @PostConstruct
    private void init() {
        executor.execute(this::produceLowPriorityMessages);
        executor.execute(this::produceHighPriorityMessages);
    }

    @PreDestroy
    private void shutdown() {
        stop.set();
        executor.shutdown();
    }

    @SneakyThrows
    private void produceLowPriorityMessages() {
        int i = 0;
        while (!Thread.currentThread().isInterrupted()) {
            var msg = String.format("low-%06d", i++);
            kafkaTemplate.send(Constants.LOW_PRIORITY_TOPIC, UUID.randomUUID().toString(), msg);
            log.info("sent low: {}", msg);

            if (i >= Constants.MAX_LOW_MESSAGES) {
                this.shutdown();
                return;
            }
            Constants.sleepAbout(Constants.LOW_MESSAGE_PRODUCING_PERIOD);
        }
    }

    @SneakyThrows
    private void produceHighPriorityMessages() {
        int i = 0;
        while (!Thread.currentThread().isInterrupted()) {
            if (stop.wait(Constants.addNoise(Constants.HIGH_MESSAGE_SLEEP_PERIOD), TimeUnit.MILLISECONDS)) {
                return;
            }

            var batchSize = Constants.addNoise(Constants.HIGH_MESSAGES_BATCH_SIZE);
            for (int j = 0; j < batchSize; j++) {
                var msg = String.format("high-%06d", i++);
                kafkaTemplate.send(Constants.HIGH_PRIORITY_TOPIC, UUID.randomUUID().toString(), msg);
                log.info("sent high: {}", msg);

                if (stop.wait(Constants.addNoise(Constants.HIGH_MESSAGE_PRODUCING_PERIOD), TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        }
    }
}
