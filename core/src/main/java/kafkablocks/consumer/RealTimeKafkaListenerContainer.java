package kafkablocks.consumer;

import kafkablocks.events.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.util.Map;
import java.util.function.Consumer;

class RealTimeKafkaListenerContainer implements KafkaListenerContainer {

    private final Logger logger;
    private final KafkaMessageListenerContainer<String, Event> container;

    RealTimeKafkaListenerContainer(
            Logger logger,
            KafkaSingleConsumer.HandleProxy handleProxy,
            Map<String, Object> consumerProps,
            String topic
    ) {
        this.logger = logger;
        ConsumerFactory<String, Event> factory = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProps = getContainerProperties(topic, handleProxy.getNewRecord());
        this.container = createListenerContainer(factory, containerProps, handleProxy.getErrorHandler());
    }

    private ContainerProperties getContainerProperties(
            String topic,
            Consumer<ConsumerRecord<String, Event>> recordConsumer
    ) {
        ContainerProperties props = new ContainerProperties(topic);

        props.setMessageListener((MessageListener<String, Event>) recordConsumer::accept);
        props.setShutdownTimeout(100);

        return props;
    }

    private KafkaMessageListenerContainer<String, Event> createListenerContainer(
            ConsumerFactory<String, Event> factory,
            ContainerProperties containerProps,
            org.springframework.kafka.listener.ErrorHandler errorHandler
    ) {
        KafkaMessageListenerContainer<String, Event> listenerContainer =
                new KafkaMessageListenerContainer<>(factory, containerProps);

        listenerContainer.setErrorHandler(errorHandler);
        listenerContainer.setEmergencyStop(
                () -> errorHandler.handle(new IllegalStateException("container emergency stop!"), null));

        return listenerContainer;
    }

    @Override
    public void start() {
        this.container.start();
    }

    @Override
    public void stop() {
        this.container.start();
    }

    @Override
    public void resume() {
        // do nothing
    }
}
