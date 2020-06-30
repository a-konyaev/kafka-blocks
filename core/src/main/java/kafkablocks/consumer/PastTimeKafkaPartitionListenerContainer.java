package kafkablocks.consumer;

import kafkablocks.concurrent.WaitHandle;
import kafkablocks.events.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.TopicPartitionOffset;

import java.util.Map;

class PastTimeKafkaPartitionListenerContainer implements KafkaListenerContainer {

    private final Logger logger;
    private final KafkaSingleConsumer.HandleProxy handleProxy;
    private final long fromTs;
    private final long toTs;
    private final KafkaMessageListenerContainer<String, Event> container;
    /**
     * Последний оффсет в топике (на момент создания лисенера)
     */
    private long lastOffset;
    /**
     * Timestamp последней записи, которую обработали
     */
    private long lastRecordTs;
    /**
     * Время, когда запустили воспроизведение для текущей (очередной) записи
     */
    private long recordPlaybackStartedAt;

    public PastTimeKafkaPartitionListenerContainer(
            Logger logger,
            KafkaSingleConsumer.HandleProxy handleProxy,
            Map<String, Object> consumerProps,
            String topic,
            PastTimeKafkaListenerContainer.Offsets offsets,
            long fromTs, long toTs
    ) {
        String loggerName = String.format("%s-%s", logger.getName(), offsets.getFrom().getTopicPartition().toString());
        this.logger = LoggerFactory.getLogger(loggerName);

        this.handleProxy = handleProxy;
        this.fromTs = fromTs;
        this.toTs = toTs;
        this.lastOffset = offsets.getLast();

        ConsumerFactory<String, Event> factory = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProps = getContainerProperties(offsets.getFrom());
        this.container = createListenerContainer(factory, containerProps, handleProxy.getErrorHandler());
    }

    @Override
    public String toString() {
        return this.logger.getName();
    }

    private ContainerProperties getContainerProperties(TopicPartitionOffset partitionOffset) {
        ContainerProperties props = new ContainerProperties(partitionOffset);

        props.setMessageListener((MessageListener<String, Event>) this::processRecord);
        // зададим таймаут, в течение которого вызов метода stop() будет блокироваться,
        // ожидая остановки контейнера. Нужно установить такой маленький таймаут, т.к.
        // метод stop() может вызываться изнутри данного класса и его блокирование не целесообразно.
        props.setShutdownTimeout(100);

        return props;
    }

    private KafkaMessageListenerContainer<String, Event> createListenerContainer(
            ConsumerFactory<String, Event> factory,
            ContainerProperties containerProps,
            ErrorHandler errorHandler) {
        KafkaMessageListenerContainer<String, Event> listenerContainer =
                new KafkaMessageListenerContainer<>(factory, containerProps);

        listenerContainer.setErrorHandler(errorHandler);
        listenerContainer.setEmergencyStop(
                () -> errorHandler.handle(new IllegalStateException("container emergency stop!"), null));

        return listenerContainer;
    }

    @Override
    public void start() {
        // TS последней записи, которой у нас еще нет, выставляем в "Дату С"
        lastRecordTs = this.fromTs;

        // время начала воспроизведения текущей (т.е. первой) записи - совпадает с временем запуска
        recordPlaybackStartedAt = System.currentTimeMillis();

        this.container.start();
    }

    @Override
    public void stop() {
        this.container.stop();
    }

    @Override
    public void resume() {
        // при возобновлении после паузы, во время которой воспроизведение не выполнялось,
        // необходимо установить время начала воспроизведения текущей записи в текущее время
        recordPlaybackStartedAt = System.currentTimeMillis();
    }

    private void handleRecord(ConsumerRecord<String, Event> record) {
        handleProxy.getNewRecord().accept(record);
    }

    private double getRate() {
        return handleProxy.getRate().get();
    }

    private void complete() {
        handleProxy.getComplete().accept(this);
    }

    private boolean isPaused() {
        return handleProxy.getIsPaused().getAsBoolean();
    }

    private int waitForStopOrPause(long timeout) {
        return handleProxy.getWaitForStopOrPause().apply(timeout);
    }

    private boolean waitForResumeOrStop() {
        return handleProxy.getWaitForResumeOrStop().getAsBoolean();
    }

    //region record processing

    private void processRecord(ConsumerRecord<String, Event> record) {
        // если контейнер уже остановлен, то ничего не делаем.
        // проверка нужна потому, что даже после остановки этот метод будет продолжать вызываться
        // для оставшихся событий из прочитанной пачки (внутренний kafkaConsumer читаем события пачками)
        if (!container.isRunning()) {
            logger.debug("skip the record because container is not running");
            return;
        }

        long recordTs = record.timestamp();
        logger.debug("lastRecordTs = {}; recordTs = {}; from = {}; to = {}", lastRecordTs, recordTs, fromTs, toTs);

        // если это событие с оффсетом меньше, чем левая граница интервала,
        // т.е. еще не добрались до "наших"
        if (recordTs < fromTs) {
            logger.debug(
                    "Skip the record because its timestamp '{}' is less then the From interval boundary '{}'",
                    recordTs, fromTs);
            return;
        }

        // если это событие с оффсетом больше, чем правая граница интервала,
        // т.е. все "наши" мы уже обработали
        if (recordTs > toTs) {
            logger.debug(
                    "Reached the record with timestamp '{}' exceeding the To interval boundary '{}'",
                    recordTs, toTs);
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
        if (record.offset() >= lastOffset) {
            logger.debug("Reached the record with offset '{}' that equals to the last offset", lastOffset);
            complete();
        }
    }

    /**
     * Ждать, когда настанет время для обработки записи.
     * Ожидание нужно для того, чтобы при обработке записей соблюсти тот же темп,
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
            double currentRate = getRate();

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
}
