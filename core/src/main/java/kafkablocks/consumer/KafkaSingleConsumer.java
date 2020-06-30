package kafkablocks.consumer;

import kafkablocks.EventTopicProperties;
import kafkablocks.events.Event;
import kafkablocks.serialization.EventDeserializer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/**
 * "Одиночный потребитель" - выполняет прием событий заданного типа из одного заданного топика
 */
public class KafkaSingleConsumer extends KafkaConsumerBase implements KafkaConsumer {

    private final Class<? extends Event> eventType;
    private final String topic;
    private final Map<String, Object> consumerProps;
    private final HandleProxy handleProxy;

    /**
     * прокси для передачи "указателей" на разные методы listener container-ам
     */
    @Getter
    @RequiredArgsConstructor
    public static class HandleProxy {
        private final Consumer<ConsumerRecord<String, Event>> newRecord;
        private final org.springframework.kafka.listener.ErrorHandler errorHandler;
        private final Supplier<Double> rate;
        private final BooleanSupplier isPaused;
        private final LongFunction<Integer> waitForStopOrPause;
        private final BooleanSupplier waitForResumeOrStop;
        @With
        private final Consumer<KafkaListenerContainer> complete;
    }

    //region Initialization

    /**
     * Конструктор
     *
     * @param eventType            тип события
     * @param eventTopicProperties резолвер, который используется для определения топика по типу события
     * @param kafkaProperties      параметры взаимодействия с Kafka
     */
    public KafkaSingleConsumer(
            Class<? extends Event> eventType,
            EventTopicProperties eventTopicProperties,
            KafkaProperties kafkaProperties
    ) {
        super(eventType.getSimpleName());

        this.eventType = eventType;
        this.topic = eventTopicProperties.resolveTopicByEventClass(eventType);
        this.consumerProps = getConsumerProperties(kafkaProperties);

        this.handleProxy = new HandleProxy(
                this::onNewRecord,
                this::onListenerContainerError,
                () -> consumingParams.getRate(),
                this::isPaused,
                this::waitForStopOrPause,
                this::waitForResumeOrStop,
                this::onListenerContainerComplete);

        logger.info("Consumer with id '{}' created", super.id);
    }

    /**
     * Получить параметры потребителя
     *
     * @param kafkaProperties параметры взаимодействия с Kafka, который
     * @return таблица с параметрами
     */
    private Map<String, Object> getConsumerProperties(KafkaProperties kafkaProperties) {
        Map<String, Object> map = kafkaProperties.buildConsumerProperties();

        map.put(ConsumerConfig.GROUP_ID_CONFIG, getConsumerGroupId(kafkaProperties));
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventDeserializer.class);

        return map;
    }

    /**
     * Получить ИД группы.
     * <p>
     * Этот ИД группы Kafka использует для того, чтобы понимать, что "вот эти вот" экземпляры потребителей
     * с одинаковым GroupId являются нодами одного и того же потребителя, который работает в многонодной конфигурации,
     * и тогда Kafka будет умно распределять события из топиков между этими нодами
     * (таким образом реализована балансировка нагрузки в Kafka).
     * <p>
     * Алгоритм выбора ИД группы следующий:
     * <ol>
     * <li>
     * Если он явно задан в параметре приложения "spring.kafka.consumer.group-id", то будет использоваться это значение
     * Данный вариант будет полезен как раз для работы в многонодной конфигурации.
     * </li>
     * <li>
     * Иначе, если не задан, то будет использоваться ИД данного экземпляра потребителя, который является уникальным
     * для каждого экземпляра (это UUID).
     * Данный вариант полезен будет в случае, когда мы создаем несколько потребителей, который подписываются на
     * один и тот же топик, но должны получать из него события независимо.
     * </li>
     * </ol>
     */
    private String getConsumerGroupId(KafkaProperties kafkaProperties) {
        String groupId = kafkaProperties.getConsumer().getGroupId();
        return groupId != null
                ? groupId
                : this.id;
    }

    //endregion

    //region Listener container

    private volatile KafkaListenerContainer listenerContainer;


    private KafkaListenerContainer createListenerContainer() {
        switch (consumingParams.getRegime()) {
            case REAL_TIME:
                return new RealTimeKafkaListenerContainer(
                        logger,
                        handleProxy,
                        consumerProps,
                        topic);

            case PAST_TIME_INTERVAL:
                return new PastTimeKafkaListenerContainer(
                        logger,
                        handleProxy,
                        consumerProps,
                        topic,
                        consumingParams.getFromTs(),
                        consumingParams.getToTs());

            default:
                throw new IllegalStateException("Unknown regime: " + consumingParams.getRegime());
        }
    }

    private void onListenerContainerError(Exception ex, ConsumerRecord<?, ?> record) {
        onError("Listener container error; record: " + getRecordLogText(record), ex);
    }

    private static String getRecordLogText(ConsumerRecord<?, ?> record) {
        return record == null
                ? "<null>"
                : String.format("[%s; p:%d; o:%d; ts:%d] %s = %s",
                record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value());
    }

    //endregion

    //region Events handling

    private EventHandler<Event> eventHandler;

    @Override
    @SuppressWarnings("unchecked")
    public <E extends Event> void setEventHandler(EventHandler<E> handler, Class<E> eventType) {
        checkEventType(eventType);
        this.eventHandler = (key, event) -> handler.process(key, (E) event);
    }

    private void checkEventType(Class<? extends Event> eventType) {
        if (this.eventType != eventType)
            throw new IllegalArgumentException(
                    String.format("Handler event type must be '%s', but it is '%s'",
                            this.eventType.getName(), eventType.getName()));
    }

    private void onNewRecord(ConsumerRecord<String, Event> record) {
        if (record.value() == null) {
            logger.warn("Record has null value: '{}'. Skip it", record.toString());
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug(getRecordLogText(record));
        }

        if (eventHandler != null) {
            try {
                eventHandler.process(record.key(), record.value());
            } catch (Exception e) {
                onError("EventHandler error while processing record " + getRecordLogText(record), e);
            }
        }
    }

    //endregion

    //region Lifecycle management

    /**
     * Время, когда потребитель был запущен
     */
    private long startedAt;

    @Override
    public void start() {
        logger.debug("starting...");

        assertNextPhase(KafkaConsumerPhase.RUNNING);

        // запоминаем время запуска
        startedAt = System.currentTimeMillis();

        listenerContainer = createListenerContainer();
        listenerContainer.start();

        setPhase(KafkaConsumerPhase.RUNNING);
        logConsumerStarted();
    }

    private void logConsumerStarted() {
        switch (consumingParams.getRegime()) {
            case REAL_TIME:
                logger.info("Consumer started at {}; mode = REAL_TIME", startedAt);
                return;

            case PAST_TIME_INTERVAL:
                logger.info(
                        "Consumer started at {}; mode = PAST_TIME_INTERVAL; from = {}; to = {}; rate = {}",
                        startedAt, consumingParams.getFromTs(), consumingParams.getToTs(), consumingParams.getRate());
                break;
        }
    }

    @Override
    public void stop() {
        stopInternal(false);
    }

    /**
     * Метод предназначен для того, чтобы внутренний listenerContainer сообщил о том,
     * что завершил работу, например, по причине того, что данные в топике кончались или топики вообще были пустыми
     */
    private void onListenerContainerComplete(KafkaListenerContainer container) {
        stopInternal(true);
    }

    /**
     * Остановка
     *
     * @param completed true - если причиной остановки является завершение работы всех контейнеров,
     *                  false - если причиной остановки является внешний вызов.
     */
    private void stopInternal(boolean completed) {
        logger.debug("stopping...");
        long workingTime = System.currentTimeMillis() - startedAt;

        // если контейнер сам завершил работу
        if (completed) {
            // фазу меняем с асинхронной нотификацией подписчиков, причем еще и с задержкой в 100 мсек
            // это нужно для того, чтобы текущий консьюмер успел завершить работу
            setPhase(KafkaConsumerPhase.STOPPED, true, 100);

        } else { // вызов остановки извне
            if (listenerContainer != null) {
                listenerContainer.stop();
            }
            setPhase(KafkaConsumerPhase.STOPPED);
        }

        listenerContainer = null;

        logger.info("Consumer stopped; reason: {}; working time: {}",
                completed ? "consuming completed" : "stop was called",
                workingTime);
    }

    @Override
    protected boolean pauseInternal() {
        return setPhase(KafkaConsumerPhase.PAUSED);
    }

    @Override
    protected void resumeInternal() {
        if (listenerContainer != null) {
            listenerContainer.resume();
        }

        setPhase(KafkaConsumerPhase.RUNNING);
    }

    //endregion
}
