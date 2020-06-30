package kafkablocks.consumer;

import kafkablocks.events.Event;
import kafkablocks.utils.ThreadUtils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.springframework.kafka.support.TopicPartitionOffset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class PastTimeKafkaListenerContainer implements KafkaListenerContainer {

    private final Logger logger;
    private final KafkaSingleConsumer.HandleProxy handleProxy;
    /**
     * key - listener, value - completed or not
     */
    private final Map<KafkaListenerContainer, Boolean> partitionListeners = new HashMap<>();
    private final Object partitionListenersSync = new Object();

    @Getter
    @RequiredArgsConstructor
    static class Offsets {
        /**
         * Offset to start reading from which
         */
        private final TopicPartitionOffset from;
        /**
         * The last offset at the topic now
         */
        private final long last;
    }

    public PastTimeKafkaListenerContainer(
            Logger logger,
            KafkaSingleConsumer.HandleProxy handleProxy,
            Map<String, Object> consumerProps,
            String topic,
            long fromTs, long toTs
    ) {
        this.logger = logger;
        this.handleProxy = handleProxy;

        // контейнерам, который читают партиции, передаем модифицированную прокси,
        // которая некоторые методы сначала проксирует в данный класс
        //todo: нужно ли также заменять newRecord, чтобы, например, в нем собирать все записи в блокин-кью,
        //  а уже из нее отдавать наружу?
        KafkaSingleConsumer.HandleProxy handleProxyForContainers =
                handleProxy.withComplete(this::onListenerContainerComplete);

        List<Offsets> offsets = findOffsets(consumerProps, topic, fromTs);
        for (Offsets offset : offsets) {
            PastTimeKafkaPartitionListenerContainer listener = new PastTimeKafkaPartitionListenerContainer(
                    logger,
                    handleProxyForContainers,
                    consumerProps,
                    topic, offset,
                    fromTs, toTs);
            this.partitionListeners.put(listener, false);
        }
    }

    private List<Offsets> findOffsets(
            Map<String, Object> consumerProps, String topic, long timestamp
    ) {
        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, Event> consumer =
                     new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps)
        ) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            Map<TopicPartition, Long> timestampMap = new HashMap<>(partitions.size());

            for (PartitionInfo partitionInfo : partitions) {
                timestampMap.put(new TopicPartition(topic, partitionInfo.partition()), timestamp);
            }

            Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timestampMap);
            Map<TopicPartition, Long> endOffsetMap = consumer.endOffsets(timestampMap.keySet());

            // в случае, если в топик пуст, то offsetsForTimes может вернуть null.
            // а если какие то партиции пустые, то соотв. значения таблицы будут null.
            if (offsetMap == null) {
                logger.debug("offsets not found");
                return Collections.emptyList();
            }

            List<Offsets> offsets = new ArrayList<>();

            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                OffsetAndTimestamp offsetAndTimestamp = entry.getValue();

                if (offsetAndTimestamp == null) {
                    logger.debug("topic partition {} is empty", topicPartition);
                    continue;
                }

                TopicPartitionOffset fromOffset = new TopicPartitionOffset(
                        topic,
                        topicPartition.partition(),
                        offsetAndTimestamp.offset());

                // todo: странный кусок - уточнить
                // -1 потому что этот offset - тот, который будет присвоен новому событию, которое поступит в топик
                long last = endOffsetMap.getOrDefault(topicPartition, 1L) - 1;

                offsets.add(new Offsets(fromOffset, last));
            }

            return offsets;
        }
    }

    @Override
    public void start() {
        if (partitionListeners.size() == 0) {
            logger.debug("No partition listeners");
            // задержка нужна для того, чтобы фаза успела получить значение RUNNING,
            // т.к. иначе complete() не сможет выставить ее равной STOPPED
            completeAsync(100);
            return;
        }

        execForEachListener(KafkaListenerContainer::start);
    }

    private void onListenerContainerComplete(KafkaListenerContainer listener) {
        synchronized (partitionListenersSync) {
            Boolean isStopped = partitionListeners.get(listener);
            if (isStopped) {
                return;
            }

            partitionListeners.put(listener, true);
            // контрольный останов контейнера
            listener.stop();
            logger.debug("listener {} completed", listener);

            boolean allStopped = partitionListeners.values().stream().allMatch(stopped -> stopped);
            if (!allStopped) {
                return;
            }
        }

        logger.debug("All listeners completed");
        completeAsync(0);
    }

    private void completeAsync(long delayMillis) {
        CompletableFuture.runAsync(() -> {
            if (delayMillis > 0) {
                ThreadUtils.safeDelay(delayMillis);
            }
            this.handleProxy.getComplete().accept(this);
        });
    }

    @Override
    public void stop() {
        execForEachListener(KafkaListenerContainer::stop);
    }

    @Override
    public void resume() {
        execForEachListener(KafkaListenerContainer::resume);
    }

    private void execForEachListener(Consumer<KafkaListenerContainer> action) {
        partitionListeners.keySet().parallelStream().forEach(action);
    }

}
