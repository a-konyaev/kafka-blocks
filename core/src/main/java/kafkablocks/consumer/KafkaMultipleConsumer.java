package kafkablocks.consumer;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.util.Assert;
import kafkablocks.events.Event;
import kafkablocks.EventTopicProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * "Множественный потребитель" - выполняет прием сообщений разных типов,
 * где для каждого типа сообщения опеределен свой топик.
 * Внутри использует KafkaSingleConsumer для каждого типа сообщения.
 */
public class KafkaMultipleConsumer extends KafkaConsumerBase implements KafkaConsumer {
    private final EventTopicProperties eventTopicProperties;
    private final KafkaProperties kafkaProperties;
    /**
     * Таблица потребителей.
     * ключ - тип события, значение - одиночный потребитель
     */
    private final Map<Class<? extends Event>, KafkaSingleConsumer> consumerMap = new HashMap<>();


    public KafkaMultipleConsumer(EventTopicProperties eventTopicProperties, KafkaProperties kafkaProperties) {
        super(StringUtils.EMPTY);

        this.eventTopicProperties = eventTopicProperties;
        this.kafkaProperties = kafkaProperties;
        logger.info("Consumer with id '{}' created", id);
    }

    @Override
    public <T extends Event> void setEventHandler(EventHandler<T> eventHandler, Class<T> eventType) {
        Assert.notNull(eventHandler, "eventHandler is null");
        KafkaSingleConsumer consumer = getOrCreateConsumer(eventType);
        consumer.setEventHandler(eventHandler, eventType);
    }

    private KafkaSingleConsumer getOrCreateConsumer(Class<? extends Event> eventType) {
        Assert.notNull(eventType, "eventType is null");

        KafkaSingleConsumer existingConsumer = consumerMap.get(eventType);
        if (existingConsumer != null)
            return existingConsumer;

        KafkaSingleConsumer consumer = new KafkaSingleConsumer(eventType, eventTopicProperties, kafkaProperties);
        consumer.setErrorHandler(error -> onError("consumer error: " + consumer.getId(), error));
        consumer.setIdleHandler(super::onIdle);
        // подписка на изменение фаз нужна, чтобы поймать момент, когда потребитель остановится
        consumer.setPhaseChangedHandler(this::onConsumerPhaseChanged);

        consumerMap.put(eventType, consumer);

        return consumer;
    }

    private void onConsumerPhaseChanged(KafkaConsumerPhase phase) {
        logger.debug("onConsumerPhaseChanged: {}", phase);

        // если потребитель, кот. вызвал метод, остановился
        if (phase == KafkaConsumerPhase.STOPPED) {
            // а также вообще все потребители завершили работу
            boolean allStopped = consumerMap.values().stream()
                    .allMatch(consumer -> consumer.getPhase() == KafkaConsumerPhase.STOPPED);
            if (allStopped) {
                //todo: если было 2 потребителя с пустыми топиками, то дернем 2 раза - надо исправить: дергать =1 раз

                // то значит и данный потребитель завершил работу
                setPhase(KafkaConsumerPhase.STOPPED);
            }
        }
    }

    @Override
    public void setIdleEventInterval(int sec) {
        execForEachConsumer(consumer -> consumer.setIdleEventInterval(sec));
    }

    @Override
    public void setConsumingParams(ConsumingParams params) {
        super.setConsumingParams(params);
        execForEachConsumer(consumer -> consumer.setConsumingParams(params));
    }

    @Override
    public void changeRate(double rate) {
        super.changeRate(rate);
        execForEachNotStoppedConsumer(consumer -> consumer.changeRate(rate));
    }

    @Override
    public void start() {
        execForEachConsumer(KafkaSingleConsumer::start);
        setPhase(KafkaConsumerPhase.RUNNING);
    }

    @Override
    public void stop() {
        execForEachNotStoppedConsumer(KafkaSingleConsumer::stop);
        setPhase(KafkaConsumerPhase.STOPPED);
    }

    @Override
    public void pause() {
        // для данного консюмера базовая реализация pause() не подходит, поэтому переопределяем ее
        execForEachNotStoppedConsumer(KafkaSingleConsumer::pause);
        pauseInternal();
    }

    @Override
    protected void pauseInternal() {
        setPhase(KafkaConsumerPhase.PAUSED);
    }

    @Override
    public void resume() {
        // для данного консюмера базовая реализация resume() не подходит, поэтому переопределяем ее
        execForEachNotStoppedConsumer(KafkaSingleConsumer::resume);
        resumeInternal();
    }

    @Override
    protected void resumeInternal() {
        setPhase(KafkaConsumerPhase.RUNNING);
    }

    @Override
    public void addFilter(Filter filter) {
        execForEachConsumer(consumer -> consumer.addFilter(filter));
    }

    @Override
    public void resetFilters() {
        execForEachConsumer(KafkaSingleConsumer::resetFilters);
    }

    private void execForEachNotStoppedConsumer(Consumer<KafkaSingleConsumer> action) {
        execForEachConsumer(action, consumer -> consumer.getPhase() != KafkaConsumerPhase.STOPPED);
    }

    private void execForEachConsumer(Consumer<KafkaSingleConsumer> action) {
        execForEachConsumer(action, null, true);
    }

    private void execForEachConsumer(Consumer<KafkaSingleConsumer> action, Predicate<KafkaSingleConsumer> filter) {
        execForEachConsumer(action, filter, true);
    }

    private void execForEachConsumer(
            Consumer<KafkaSingleConsumer> action,
            Predicate<KafkaSingleConsumer> filter,
            boolean parallel) {

        Collection<KafkaSingleConsumer> consumers = consumerMap.values();
        Stream<KafkaSingleConsumer> stream = parallel ? consumers.parallelStream() : consumers.stream();

        stream.forEach(consumer -> {

            try {
                if (filter != null && !filter.test(consumer))
                    // не прошли условие выполнения action
                    return;

            } catch (Throwable e) {
                throw new RuntimeException("Error while testing predicate for consumer: " + consumer.getId(), e);
            }

            try {
                action.accept(consumer);
            } catch (Throwable e) {
                throw new RuntimeException("Error while performing action for consumer: " + consumer.getId(), e);
            }
        });
    }
}
