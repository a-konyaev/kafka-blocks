package kafkablocks.examples.prioritizer.consumer2;

import kafkablocks.examples.prioritizer.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kafkablocks.examples.prioritizer.Constants.HIGH_PRIORITY_TOPIC;
import static kafkablocks.examples.prioritizer.Constants.LOW_PRIORITY_TOPIC;

@Service
@Slf4j
public class Consumer2 {
    @Autowired
    private BusinessService businessService;
    @Autowired
    private TopicPartitionInfo topicPartitionInfo;

    private long startTs;
    private int lowMsgCount;

    @KafkaListener(
            id = "consumer2-group",
            topics = {HIGH_PRIORITY_TOPIC, LOW_PRIORITY_TOPIC},
            containerFactory = "myFactory")
    public void getMessagesCalculatingOffset(@Payload String body,
                                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                             Consumer<?, ?> consumer,
                                             Acknowledgment ack) {

        if (lowMsgCount == 0) {
            startTs = System.currentTimeMillis();
        }

        if (HIGH_PRIORITY_TOPIC.equals(topic)) {
            businessService.processHigh(body);
            ack.acknowledge();
            return;
        }

        if (LOW_PRIORITY_TOPIC.equals(topic)) {

            var noUnread = noUnreadMessagesInTopic(HIGH_PRIORITY_TOPIC, consumer);

            if (noUnread) {
                businessService.processLow(body);
                ack.acknowledge();

                if (++lowMsgCount == Constants.MAX_LOW_MESSAGES) {
                    var duration = System.currentTimeMillis() - startTs;
                    log.info("processed {} low messages in {} msec", lowMsgCount, duration);
                }

            } else {
                ack.nack(0);
            }
        }
    }

    private boolean noUnreadMessagesInTopic(String topic, Consumer<?, ?> consumer) {
        Collection<TopicPartition> partitions;
        //partitions = getTopicPartitions(topic, consumer);
        partitions = topicPartitionInfo.getTopicPartitions(topic);

        // todo: этот вызов периодически занимает до 500 мсек!
        //var s = System.currentTimeMillis();
        Map<TopicPartition, Long> endingOffsets = consumer.endOffsets(partitions);
        //log.info("consumer.endOffsets {} msec", (System.currentTimeMillis() - s));

        Map<TopicPartition, Long> currentOffsets = new HashMap<>();
        partitions.forEach(topicPartition -> currentOffsets.put(topicPartition, consumer.position(topicPartition)));
        return (currentOffsets.equals(endingOffsets));
    }

    private Collection<TopicPartition> getTopicPartitions(String topic, Consumer<?, ?> consumer) {
        // todo: этот вызов периодически занимает до 100 мсек!
        List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
        log.info("partitionInfos = {}", partitionInfos);

        Collection<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
            partitions.add(partition);
        }

        return partitions;
    }
}
