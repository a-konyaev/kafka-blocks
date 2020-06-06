package kafkablocks.examples;

import kafkablocks.examples.springkafka.consumer.SpringConsumer;
import kafkablocks.examples.springkafka.consumer.SpringConsumerApp;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest(classes = SpringConsumerApp.class)
@ExtendWith(SpringExtension.class)
@Import(TestConfig.class)
@Slf4j
@ContextConfiguration(initializers = KafkaInitializer.class)
class SpringConsumerTest {

    @SpyBean
    private SpringConsumer consumer;

    @Test
    void listen() {
        log.debug("!!!");
    }
}