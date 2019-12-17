package kafkablocks.examples;

import kafkablocks.ServiceBase;
import kafkablocks.concurrent.WaitHandle;
import kafkablocks.examples.events.PositionEvent;
import kafkablocks.publisher.KafkaPublisher;
import kafkablocks.utils.RandomUtils;
import kafkablocks.utils.ThreadUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
@Slf4j
public class PositionEventPublisher extends ServiceBase {
    private final KafkaPublisher kafkaPublisher;

    private static final int THREAD_COUNT = 3;
    private final List<Thread> threads = new ArrayList<>(THREAD_COUNT);
    private final WaitHandle stoppedEvent = new WaitHandle();

    @Override
    protected void init() {
        for (int i = 0; i < THREAD_COUNT; i++) {
            var thread = ThreadUtils.startNewThread(this::run, "publisher-" + i);
            threads.add(thread);
        }
    }

    @Override
    protected void shutdown() {
        stoppedEvent.set();
        threads.forEach(ThreadUtils::joinThread);
    }

    private void run() {
        final var objectId = UUID.randomUUID().toString();
        log.info("starting event generating for object '{}'", objectId);

        while (true) {
            var millis = RandomUtils.getRandomInt(500, 1000);
            var hasStopped = stoppedEvent.wait(millis, TimeUnit.MILLISECONDS);
            if (hasStopped) {
                break;
            }

            var event = new PositionEvent(objectId, 0, 0);
            kafkaPublisher.publishEvent(event);
        }

        log.info("event generating for object '{}' finished", objectId);
    }
}
