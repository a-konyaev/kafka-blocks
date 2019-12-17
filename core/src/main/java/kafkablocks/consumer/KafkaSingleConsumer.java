package kafkablocks.consumer;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import kafkablocks.concurrent.WaitHandle;
import kafkablocks.events.Event;
import kafkablocks.EventTopicProperties;
import kafkablocks.serialization.EventDeserializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * "Одиночный потребитель" - выполняет прием событий заданного типа из одного заданного топика
 */
public class KafkaSingleConsumer extends KafkaConsumerBase implements KafkaConsumer {
    private final Class<? extends Event> eventType;
    private final String topic;
    /**
     * Контейнер-прослушиватель топика
     */
    private final GenericMessageListenerContainer<String, Event> listenerContainer;
    /**
     * Низкоуровневый потребитель Kafka
     */
    private Consumer<?, ?> consumer;
    /**
     * Последний оффсет в топике на момент запуска
     */
    private long lastOffsetAtStart;
    /**
     * Время, когда потребитель был запущен
     */
    private long startedAt;
    /**
     * Timestamp последней записи, которую обработали
     */
    private long lastRecordTs;
    /**
     * Время, когда запустили воспроизведение для текущей (очередной) записи
     */
    private long recordPlaybackStartedAt;

    private EventHandler<Event> eventHandler;


    //region Initialization

    /**
     * Конструктор
     *
     * @param eventType            тип события
     * @param eventTopicProperties резолвер, который используется для определения топика по типу события
     * @param kafkaProperties      параметры взаимодействия с Kafka
     */
    public KafkaSingleConsumer(
            Class<? extends Event> eventType, EventTopicProperties eventTopicProperties, KafkaProperties kafkaProperties) {
        super(eventType.getSimpleName());

        this.eventType = eventType;
        this.topic = eventTopicProperties.resolveTopicByEventClass(eventType);
        this.listenerContainer = createListenerContainer(getConsumerProperties(kafkaProperties));

        logger.info("Consumer with id '{}' created", id);
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

    /**
     * Создать контейнер для прослушивания топика
     *
     * @param consumerProperties параметры потребителя
     * @return контейнер-прослушиватель
     */
    private GenericMessageListenerContainer<String, Event> createListenerContainer(
            Map<String, Object> consumerProperties) {

        ConsumerFactory<String, Event> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties);

        ContainerProperties containerProps = new ContainerProperties(topic);

        // этот параметр задаем для того, чтобы сохранить ссылку на "низкоуровневый" потребитель кафки,
        // который нам нужен для выполнения seek-а
        containerProps.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                setConsumer(consumer);
            }
        });
        containerProps.setMessageListener(new InternalMessageListener());
        // таймаут простоя (когда новые события не поступают)
        containerProps.setIdleEventInterval(idleEventInterval * 1000L);
        // зададим таймаут, в течение которого вызов метода stop() будет блокироваться,
        // ожидая остановки контейнера. Нужно установить такой маленький таймаут, т.к.
        // метод stop() может вызываться изнутри данного класса и его блокирование не целесообразно.
        containerProps.setShutdownTimeout(100);

        AbstractMessageListenerContainer<String, Event> container =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.setErrorHandler((error, record) ->
                onError("container error while processing record " + getRecordLogText(record), error));

        return container;
    }

    private void setConsumer(Consumer<?, ?> consumer) {
        // note: этот метод вызывается много раз... видимо, где то внутри создается несколько консьюмеров,
        // а также после запуска контейнера
        logger.debug("remember consumer: {}", consumer.hashCode());
        this.consumer = consumer;
    }

    /**
     * Внутрений слушатель событий
     */
    private class InternalMessageListener implements MessageListener<String, Event>, ConsumerSeekAware {
        @Override
        public void onMessage(ConsumerRecord<String, Event> data) {
            processRecord(data);
        }

        @Override
        public void registerSeekCallback(ConsumerSeekCallback callback) {
        }

        @Override
        public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback seekCallback) {
            try {
                initialSeek(assignments, seekCallback);

                if (logger.isDebugEnabled()) {
                    logger.debug("Warm-up done (for {} ms)", System.currentTimeMillis() - startedAt);
                }

            } catch (Throwable e) {
                onError("initialSeek failed", e);
            }
        }

        @Override
        public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
            onIdle();
        }
    }

    /**
     * Первоначальная установка "указателя" начала чтения событий из топика
     */
    private void initialSeek(
            Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback seekCallback) {

        logger.debug("seeking...");

        // todo: этот алгоритм не будет работать, если у топика будет несколько партиций.
        // нужно хранить разные текущие метрики отдельно для каждой партиции, например lastOffsetAtStart

        // логируем информацию о партициях топика
        for (Map.Entry<TopicPartition, Long> assignment : assignments.entrySet()) {
            // -1 потому что этот offset - тот, который будет присвоен новому событию, которое поступит в топик
            lastOffsetAtStart = assignment.getValue() - 1;
            logTopicPartitionOffset("partition assigned", assignment.getKey(), assignment.getValue());
        }

        if (consumingParams.getRegime() == ConsumingRegime.REAL_TIME) {
            logger.debug("Realtime mode, no need to seeking");
            return;
        }

        boolean seekDone = false;
        Map<TopicPartition, OffsetAndTimestamp> offsetsFrom = searchOffset(consumingParams.getFromTs());
        if (offsetsFrom != null) {
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsFrom.entrySet()) {
                OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
                if (offsetAndTimestamp == null) {
                    continue;
                }

                TopicPartition topicPartition = entry.getKey();
                long offset = offsetAndTimestamp.offset();
                // устанавливаем указатель начала чтения для партиции топика на найденный оффсет
                seekCallback.seek(topicPartition.topic(), topicPartition.partition(), offset);
                logTopicPartitionOffset("partition seek", topicPartition, offset);

                seekDone = true;
            }
        }

        if (seekDone) {
            // определим оффсет "конца" интервала чтения (нужно для отладки)
            boolean finishOffsetFound = false;
            Map<TopicPartition, OffsetAndTimestamp> offsetsTo = searchOffset(consumingParams.getToTs());
            if (offsetsTo != null) {
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsTo.entrySet()) {
                    OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
                    if (offsetAndTimestamp == null) {
                        continue;
                    }

                    long offset = offsetAndTimestamp.offset();
                    logger.debug("finish offset: {}", offset);
                    finishOffsetFound = true;
                }
            }

            if (!finishOffsetFound) {
                // мы нашли старотовый offset, но не нашли финишный
                // => все события, которые есть топике (а они там есть), попали внутрь интервала From-To
                logger.debug("finish offset NOT FOUND!");
            }
        } else {
            logger.debug("No seeks has been done");
            complete();
        }

        logger.debug("seeking...done");
    }

    /**
     * Найти оффсет (указатель) события в топике для заданного времени
     *
     * @param timestamp временная метка
     * @return таблица {партиция - номер оффсета} или Null, если оффсеты не были найдены
     * (когда, например, топик пуст)
     */
    private Map<TopicPartition, OffsetAndTimestamp> searchOffset(long timestamp) {
        logger.debug("searching offsets for timestamp: {}", timestamp);

        Consumer<?, ?> tmpConsumer = this.consumer;

        List<PartitionInfo> partitionInfos = tmpConsumer.partitionsFor(topic);
        Map<TopicPartition, Long> seekMap = new HashMap<>(partitionInfos.size());

        for (PartitionInfo partitionInfo : partitionInfos) {
            seekMap.put(new TopicPartition(topic, partitionInfo.partition()), timestamp);
        }
        return tmpConsumer.offsetsForTimes(seekMap);
    }

    private void logTopicPartitionOffset(String prefixMsg, TopicPartition tp, long offset) {
        logger.debug("{}: topic = {}, partition = {}, offset = {}", prefixMsg, tp.topic(), tp.partition(), offset);
    }

    //endregion

    //region Records processing

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

    /**
     * Обработка записи события
     */
    private void handleRecord(ConsumerRecord<String, Event> record) {
        logger.debug("handling record...");

        if (eventHandler != null) {
            try {
                eventHandler.process(record.key(), record.value());
            } catch (Exception e) {
                onError("EventHandler error while processing record " + getRecordLogText(record), e);
            }
        }
    }

    private String getRecordLogText(ConsumerRecord<?, ?> record) {
        return record == null
                ? "<null>"
                : String.format("[%s # %s] %s = %s", record.offset(), record.timestamp(), record.key(), record.value());
    }

    /**
     * Обработать событие, которое прочитали из топика
     *
     * @param record запись события
     */
    private void processRecord(ConsumerRecord<String, Event> record) {
        if (record.value() == null) {
            logger.warn("Record has null value: '{}'. Skip it", record.toString());
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug(getRecordLogText(record));
        }

        // если контейнер уже остановлен, то ничего не делаем.
        // проверка нужна потому, что даже после остановки этот метод будет продолжать вызываться
        // для оставшихся событий из прочитанной пачки (внутренний kafkaConsumer читаем события пачками)
        if (!listenerContainer.isRunning()) {
            logger.debug("skip the record because container is not running");
            return;
        }

        if (consumingParams.getRegime() == ConsumingRegime.REAL_TIME) {
            handleRecord(record);
            return;
        }
        // else - ConsumingParams.ConsumingRegime.PAST_TIME_INTERVAL

        long recordTs = record.timestamp();
        if (logger.isDebugEnabled()) {
            logger.debug("lastRecordTs = {}; recordTs = {}; from = {}; to = {}",
                    lastRecordTs, recordTs, consumingParams.getFromTs(), consumingParams.getToTs());
        }

        // если это событие с оффсетом меньше, чем левая граница интервала,
        // т.е. еще не добрались до "наших"
        if (recordTs < consumingParams.getFromTs()) {
            logger.debug(
                    "Skip the record because its timestamp '{}' is less then the From interval boundary '{}'",
                    recordTs, consumingParams.getFromTs());
            return;
        }

        // если это событие с оффсетом больше, чем правая граница интервала,
        // т.е. все "наши" мы уже обработали
        if (recordTs > consumingParams.getToTs()) {
            logger.debug(
                    "Reached the record with timestamp '{}' exceeding the To interval boundary '{}'",
                    recordTs, consumingParams.getToTs());
            complete();
            return;
        }

        if (!waitForRecordTime(recordTs)) {
            logger.debug("consumer was stopped while sleeping -> interrupt processing");
            return;
        }
        // время для обработки записи настало!

        // но сперва проверим, есть ли сейчас пауза, т.к. мы могли обрабатывать записи без ожидания,
        // т.к. не успевали из-за быстрой скорости поступления записей и/или большого времени их обработки
        if (isPaused()) {
            logger.debug("consumer was paused -> suspend processing until resume or stop");

            if (!waitForResumeOrStop()) {
                logger.debug("consumer was stopped after pause -> interrupt processing");
                return;
            }

            logger.debug("consumer was resumed -> continue processing");
        }

        // запоминаем TS записи
        lastRecordTs = recordTs;
        // установим время начала воспроизведения для следующей записи в текущее время
        // ! важно сделать это именно ДО вызова обработчика текущей записи, т.к. на обработку может уйти какое то время
        recordPlaybackStartedAt = System.currentTimeMillis();

        // обрабатываем запись
        handleRecord(record);

        // эта проверка нужна для того, чтобы завершить обработку, если в топике больше нет событий вообще
        if (record.offset() >= lastOffsetAtStart) {
            logger.debug(
                    "Reached the record with offset '{}' that equals to the last offset at start time",
                    lastOffsetAtStart);
            complete();
        }
    }

    /**
     * Ждать, когда настанет время для обработки записи.
     * Ожидание нужно для того, чтобы при обработки записей соблюсти тот же темп,
     * который был во время их realtime-поступления.
     * Темп соблюдается с точностью до коэф. скорости воспроизведения, т.е.
     * если rate = 2.0, то события будут передаваться обработчику в 2 раза быстрее,
     * а при rate = 0.5 - в два раза медленнее, соответственно.
     *
     * @param recordTs временная метка записи
     * @return true - время для обработки настало, false - обработка была прервана
     */
    private boolean waitForRecordTime(long recordTs) {
        // переменная, в которой накапливаем время, которое уже проспали (см. ниже)
        long readyDelta = 0;

        // цикл нужен потому, что во время ожидания может случиться пауза,
        // причем не один раз (пауза -> возобновление -> снова пауза и т.д.)
        while (true) {
            // Вычисляет время delta, на которое нужно уснуть, по формуле:
            //      delta = ((R_i - (R_last + readyDelta)) / rate) - (now - R_i_playback_started)
            // где:
            //  R_i             - временная метка записи, которая следующая в очереди на обработку, но еще не обработана
            //  R_last          - временная метка последней (предыдущей) обработанной записи
            //  readyDelta      - часть delta, которую уже успели поспать. используется в случае,
            //                      когда delta-у спим не сразу всю, а по частям из-за паузы.
            //  rate            - коэф. скорости воспроизведения
            //  now             - текущее время
            //  R_i_playback_started - время начала воспроизведения для текущей записи

            // текущее время до начала ожидания
            long curTimeBeforeSleeping = System.currentTimeMillis();
            // текущая скорость
            // NOTE: может измениться после паузы, поэтому важно получить актуальное значение
            // непосредственно перед вычислением дельты
            double currentRate = consumingParams.getRate();

            // чистая дельта, которую нужно поспать с учетом текущей скорости воспроизведения
            long delta = (long) ((recordTs - (lastRecordTs + readyDelta)) / currentRate);
            logger.debug("delta = ({} - ({} + {})) / {} = {}", recordTs, lastRecordTs, readyDelta, currentRate, delta);

            // вычислим сколько прошло времени с момента начала воспроизведения текущей записи,
            // т.е. это фактически то время, которое мы должны были бы спать,
            // но потратили на работу обработчика предыдущей записи
            long timePassedSinceRecordPlaybackStarted = curTimeBeforeSleeping - recordPlaybackStartedAt;
            // а теперь вычтем это время из дельты, чтобы доспать только оставшееся время
            delta -= timePassedSinceRecordPlaybackStarted;
            // и сразу же добавим в копилку уже времени, которое уже проспали,
            // причем в масштабе "1 к 1", поэтому умножим на текущую скорость
            readyDelta += timePassedSinceRecordPlaybackStarted * currentRate;

            logger.debug("timePassedSinceRecordPlaybackStarted = {}; readyDelta = {}; delta = {}",
                    timePassedSinceRecordPlaybackStarted, readyDelta, delta);

            if (delta <= 0) {
                // delta может получится не положительной и это нормально.
                // Такое может быть, если события поступают со скоростью выше, чем скорость их обработки,
                // или сразу после запуска, когда в самом начале требуется какое то время на "разогрев".
                logger.debug("no need to sleep, delta = {}", delta);
                return true;
            }

            // sleep for delta but wake up when stop or pause event happens
            logger.debug("sleep for delta = {}", delta);
            int eventIndex = waitForStopOrPause(delta);

            if (eventIndex == WaitHandle.WAIT_TIMEOUT) {
                logger.debug("sleeping is finished");
                return true;
            }

            if (eventIndex == 0) {
                logger.debug("Stop event occurred while sleeping");
                return false;
            }

            if (eventIndex == 1) {
                logger.debug("Pause event occurred while sleeping -> suspend processing until resume or stop event");

                // запомним ту часть времени от delta, которую мы уже проспали, также в масштабе "1 к 1"
                long sleptTime = System.currentTimeMillis() - curTimeBeforeSleeping;
                long deltaIncrement = (long) (sleptTime * currentRate);
                readyDelta += deltaIncrement;
                logger.debug("sleptTime = {}; deltaIncrement = {}; readyDelta = {}", sleptTime, deltaIncrement, readyDelta);

                if (!waitForResumeOrStop()) {
                    logger.debug("Stop event occurred while sleeping is paused");
                    return false;
                }

                logger.debug("consumer was resumed -> continue sleeping");
            }
        }
    }

    //endregion

    //region Lifecycle management

    @Override
    public void start() {
        logger.debug("starting...");

        // запоминаем время запуска
        startedAt = System.currentTimeMillis();;
        // TS последней записи, которой у нас еще нет, выставляем в "Дату С"
        lastRecordTs = consumingParams.getFromTs();
        // время начала воспроизведения текущей (т.е. первой) записи - совпадает с временем запуска
        recordPlaybackStartedAt = startedAt;

        listenerContainer.start();
        setPhase(KafkaConsumerPhase.RUNNING);

        if (consumingParams.getRegime() == ConsumingRegime.REAL_TIME)
            logger.info("Consumer started at {}; mode = REAL_TIME", startedAt);
        else
            logger.info("Consumer started at {}; mode = PAST_TIME_INTERVAL; from = {}; to = {}; rate = {}",
                    startedAt, consumingParams.getFromTs(), consumingParams.getToTs(), consumingParams.getRate());
    }

    @Override
    public void stop() {
        stopInternal(false);
    }

    private void complete() {
        stopInternal(true);
    }

    private void stopInternal(boolean completed) {
        logger.debug("stopping...");
        long workingTime = System.currentTimeMillis() - startedAt;

        listenerContainer.stop();

        if (completed) {
            // фазу меняем с асинхронной нотификацией подписчиков, причем еще и с задержкой в 100 мсек
            // это нужно для того, чтобы текущий консьюмер успел завершить работу
            setPhase(KafkaConsumerPhase.STOPPED, true, 100);
        } else {
            setPhase(KafkaConsumerPhase.STOPPED);
        }

        logger.info("Consumer stopped; completed = {}; working time = {}", completed, workingTime);
    }

    @Override
    protected void pauseInternal() {
        setPhase(KafkaConsumerPhase.PAUSED);
    }

    @Override
    protected void resumeInternal() {
        // при возобновлении после паузы, во время которой воспроизведение не выполнялось,
        // необходимо установить время начала воспроизведения текущей записи в текущее время
        recordPlaybackStartedAt = System.currentTimeMillis();

        setPhase(KafkaConsumerPhase.RUNNING);
    }

    //endregion

    //region Filtering

    @Override
    public void addFilter(Filter filter) {
        //todo
        throw new NotImplementedException("addFilter");
    }

    @Override
    public void resetFilters() {
        //todo
        throw new NotImplementedException("resetFilters");
    }

    //endregion
}
