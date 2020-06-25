package kafkablocks.examples.prioritizer.consumer;

import kafkablocks.concurrent.WaitHandle;
import kafkablocks.examples.prioritizer.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
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
    private final MessageChannel highChannel = new MessageChannel(
            Constants.CHANNEL_SEND_TIMEOUT, Constants.CHANNEL_RECEIVE_TIMEOUT);
    private final MessageChannel lowChannel = new MessageChannel(
            Constants.CHANNEL_SEND_TIMEOUT, Constants.CHANNEL_RECEIVE_TIMEOUT);

    private long startTs;
    private int lowMsgCount;

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

                if (++lowMsgCount == Constants.MAX_LOW_MESSAGES) {
                    var duration = System.currentTimeMillis() - startTs;
                    log.info("processed {} low messages in {} msec", lowMsgCount, duration);
                }
            }
        }
    }

    private void processMessage(String message) {
        if (lowMsgCount == 0) {
            startTs = System.currentTimeMillis();
        }

        log.info("processing message: {}", message);
        Constants.sleepAbout(Constants.MESSAGE_PROCESSING_TIME);
    }

    @KafkaListener(id = "spring-kafka-consumer-high", topics = "high-priority", containerFactory = "myFactory")
    public void listenHigh(@Payload String message,
                           @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                           Acknowledgment ack) {
        //log.info("[{}] new message: {}", partition, message);
        boolean received = highChannel.send(message);
        if (received) {
            ack.acknowledge();
        } else {
            ack.nack(0);
        }
    }

    @KafkaListener(id = "spring-kafka-consumer-low", topics = "low-priority", containerFactory = "myFactory")
    public void listenLow(@Payload String message,
                          @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                          Acknowledgment ack) {
        //log.info("[{}] new message: {}", partition, message);
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
