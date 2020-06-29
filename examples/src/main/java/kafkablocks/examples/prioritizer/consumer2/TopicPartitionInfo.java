package kafkablocks.examples.prioritizer.consumer2;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
public class TopicPartitionInfo implements ConsumerRebalanceListener {
    private final Map<String, Collection<TopicPartition>> topicPartitionMap = new HashMap<>(); //todo or ConcurrentHashMap?

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            var topicPartitions = topicPartitionMap.get(partition.topic());
            if (topicPartitions != null) {
                topicPartitions.remove(partition);
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            topicPartitionMap.computeIfAbsent(partition.topic(), k -> new ArrayList<>()).add(partition);
        }
    }

    public Collection<TopicPartition> getTopicPartitions(String topic) {
        return topicPartitionMap.get(topic);
    }
}
