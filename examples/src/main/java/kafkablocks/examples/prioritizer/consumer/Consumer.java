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
            //log.info("1111");
            var highMessages = highChannel.receive();
            //log.info("2222");
            if (!highMessages.isEmpty()) {
                //?? lowConsumer.pause();

                for (String message : highMessages) {
                    //log.info("3333");
                    processMessage(message);
                    //log.info("4444");
                }
                //log.info("5555");
                continue; // снова пробуем взять high-priority сообщения
            }

            //log.info("6666");

            //?? lowConsumer.resume();
            var lowMessages = lowChannel.receive();
            //log.info("7777");
            for (String message : lowMessages) {
                //log.info("8888");
                processMessage(message);
                //log.info("9999");
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
        //log.info("aaa");
        boolean received = highChannel.send(message);
        //log.info("bbb");
        if (received) {
            //log.info("ccc");
            ack.acknowledge();
        } else {
            //log.info("ddd");
            ack.nack(0);
        }
        //log.info("eee");
    }

    @KafkaListener(id = "spring-kafka-consumer-low", topics = "low-priority", containerFactory = "myFactory")
    public void listenLow(@Payload String message,
                          @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                          Acknowledgment ack) {
        //log.info("iii");
        boolean received = lowChannel.send(message);
        //log.info("jjj");
        if (received) {
            //log.info("kkk");
            ack.acknowledge();
            //log.info("ppp");
        } else {
            //log.info("mmm");
            ack.nack(0);
            //log.info("nnn");
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
            var res = msgReceived.wait(sendTimeout, TimeUnit.MILLISECONDS);
            msgAvailable.reset();
            return res;
        }

        public List<String> receive() {
            if (!msgAvailable.wait(receiveTimeout, TimeUnit.MILLISECONDS)) {
                return Collections.emptyList();
            }

            var msg = this.message;
            var res = Collections.singletonList(msg);
            msgReceived.set();

            return res;
        }
    }
}
