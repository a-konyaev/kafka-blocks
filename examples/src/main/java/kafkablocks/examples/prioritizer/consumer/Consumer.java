package kafkablocks.examples.prioritizer.consumer;

import kafkablocks.concurrent.WaitHandle;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class Consumer {

    private Thread thread;
    private MessageChannel highChannel = new MessageChannel(50, 200);
    private MessageChannel lowChannel = new MessageChannel(50, 200);

    @PostConstruct
    private void init() {
        thread = new Thread(this::process);
        thread.start();
    }

    @PreDestroy
    private void shutdown() {
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void process() {
        while (!Thread.currentThread().isInterrupted()) {
            var highMessages = highChannel.receive();
            if (!highMessages.isEmpty()) {
                //?? lowConsumer.pause();

                for (String message : highMessages) {
                    processMessage(message);
                }
                continue; // снова пробуем взять high-priority сообщения
            }

            //?? lowConsumer.resume();
            var lowMessages = lowChannel.receive();
            for (String message : lowMessages) {
                processMessage(message);
            }
        }
    }

    @SneakyThrows
    private void processMessage(String message) {
        log.info("processing message: {}", message);
        Thread.sleep(50);
    }

    @KafkaListener(id = "spring-kafka-consumer-high", topics = "high-priority", containerFactory = "myFactory")
    public void listenHigh(@Payload String message, Acknowledgment ack) {
        //log.info("high message: {}", message);
        boolean received = highChannel.send(message);
        if (received) {
            ack.acknowledge();
        } else {
            ack.nack(100);
        }
    }

    @KafkaListener(id = "spring-kafka-consumer-low", topics = "low-priority", containerFactory = "myFactory")
    public void listenLow(@Payload String message, Acknowledgment ack) {
        //log.info("low message: {}", message);
        boolean received = lowChannel.send(message);
        if (received) {
            ack.acknowledge();
        } else {
            ack.nack(0);
        }
    }

    @RequiredArgsConstructor
    static class MessageChannel {
        private final WaitHandle msgAvailable = new WaitHandle();
        private final WaitHandle msgReceived = new WaitHandle();
        private final long sendTimeout;
        private final long receiveTimeout;
        private volatile String message;

        public boolean send(String message) {
            msgReceived.reset();
            this.message = message;
            msgAvailable.set();
            var res = msgReceived.wait(receiveTimeout, TimeUnit.MILLISECONDS);
            msgAvailable.reset();
            return res;
        }

        public List<String> receive() {
            if (!msgAvailable.wait(sendTimeout, TimeUnit.MILLISECONDS)) {
                return Collections.emptyList();
            }

            var msg = this.message;
            var res = Collections.singletonList(msg);
            msgReceived.set();

            return res;
        }
    }
}
