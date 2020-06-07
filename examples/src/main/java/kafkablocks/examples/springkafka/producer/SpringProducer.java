package kafkablocks.examples.springkafka.producer;

import kafkablocks.events.Event;
import kafkablocks.examples.events.PositionEventGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
@RequiredArgsConstructor
@Slf4j
@EnableScheduling
public class SpringProducer {
    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final PositionEventGenerator positionEventGenerator;

    @Scheduled(fixedDelayString = "${kafkablocks.examples.springkafka.producer.interval}")
    private void run() {
        var event = positionEventGenerator.getNextEvent();
        var future = kafkaTemplate.send(SpringProducerApp.TOPIC, event.getKey(), event);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(final SendResult<String, Event> message) {
                var meta = message.getRecordMetadata();
                log.info("Event was successfully sent: key={}, partition={}, offset={}",
                        event.getKey(), meta.partition(), meta.offset());
            }

            @Override
            public void onFailure(@NonNull final Throwable throwable) {
                log.error("Unable to send event", throwable);
            }
        });
    }
}
