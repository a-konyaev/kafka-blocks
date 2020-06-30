package kafkablocks.consumer;

import kafkablocks.concurrent.WaitHandle;
import kafkablocks.utils.ThreadUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Базовый класс "Потребителя"
 */
public abstract class KafkaConsumerBase implements KafkaConsumer {
    protected final Logger logger;
    @Getter
    protected final String id;
    ConsumingParams consumingParams;
    @Setter
    protected ErrorHandler errorHandler;

    /**
     * Конструктор
     *
     * @param idPrefix префикс, который используется при формировании идентификатора экземпляра
     */
    KafkaConsumerBase(String idPrefix) {
        Class<?> clazz = getClass();

        this.id = String.format("%s-%s",
                StringUtils.isEmpty(idPrefix) ? clazz.getSimpleName() : idPrefix,
                UUID.randomUUID().toString());

        // в имени логера отчечем последние 28 символов UUID-части, чтобы было удобнее читать логи
        String loggerName = String.format("%s.%s", clazz.getName(), id.substring(0, id.length() - 28));
        this.logger = LoggerFactory.getLogger(loggerName);

        this.consumingParams = ConsumingParamsBuilder.realtime();
    }

    @Override
    public void setConsumingParams(ConsumingParams consumingParams) {
        Assert.notNull(consumingParams, "consumingParams is null");
        this.consumingParams = consumingParams;
    }

    @Override
    public void changeRate(double rate) {
        // Скорость изменяем через паузу, потому что при изменении скорости меняются
        // временные характеристики потребления и трекинга и, если не встать на паузу,
        // то возникает рассинхронизация потребления и трекинга.
        // При этом паузу и возобновление выполняем через внутренние методы,
        // т.к. их логика может отличаться от соотв. интерфейсных методов.
        logger.debug("changeRate...");

        boolean paused = pauseInternal();

        consumingParams.setRate(rate);
        adjustPlaybackTimeTrackingParams(rate);

        if (paused) {
            resumeInternal();
        }

        logger.debug("changeRate...done");
    }

    @Override
    public void pause() {
        Assert.isTrue(
                this.consumingParams.getRegime() == ConsumingRegime.PAST_TIME_INTERVAL,
                "Pause is available for consuming type PAST_TIME_INTERVAL only!");
        logger.debug("pause...");
        pauseInternal();
        logger.debug("pause...done");
    }

    protected abstract boolean pauseInternal();

    @Override
    public void resume() {
        logger.debug("resuming...");
        resumeInternal();
        logger.debug("resuming...done");
    }

    protected abstract void resumeInternal();

    void onError(String message, Throwable error) {
        logger.error("Error has occurred: " + message, error);
        runHandler(errorHandler, () -> errorHandler.onError(error), (ErrorHandler) null);
    }

    //region Phases and phase-switch-events

    @Getter
    protected volatile KafkaConsumerPhase phase = KafkaConsumerPhase.NOT_STARTED;
    private final Object phaseMonitor = new Object();

    @Setter
    protected PhaseChangedHandler phaseChangedHandler;
    /**
     * Событие "Остановка"
     */
    private final WaitHandle stoppedEvent = new WaitHandle();
    /**
     * Событие "Пауза"
     */
    private final WaitHandle pausedEvent = new WaitHandle();
    /**
     * Событие "Возобновление"
     */
    private final WaitHandle resumedEvent = new WaitHandle();

    boolean setPhase(KafkaConsumerPhase newPhase) {
        return setPhase(newPhase, false, 0);
    }

    void assertNextPhase(KafkaConsumerPhase nextPhase) {
        KafkaConsumerPhase tmp = this.phase;
        tmp.assertNextPhase(nextPhase);
    }

    boolean setPhase(KafkaConsumerPhase newPhase, boolean notifyAsync, long notifyDelay) {
        synchronized (phaseMonitor) {
            if (phase == newPhase) {
                logger.debug("Attempt to set a new phase equal to the current phase: {}", newPhase);
                return false;
            }

            phase = phase.assertNextPhase(newPhase);
        }

        logger.debug("Set phase to {}", newPhase);

        switch (newPhase) {
            case NOT_STARTED:
                break;

            case RUNNING:
                stoppedEvent.reset();

                // in case it resumed after a pause
                pausedEvent.reset();
                resumedEvent.set();

                initPlaybackTimeTracking();
                break;

            case PAUSED:
                resumedEvent.reset();
                pausedEvent.set();
                break;

            case STOPPED:
                stoppedEvent.set();

                shutdownPlaybackTimeTracking();
                break;
        }

        if (notifyAsync) {
            CompletableFuture.runAsync(() -> {
                if (notifyDelay > 0)
                    ThreadUtils.safeDelay(notifyDelay);

                runHandler(
                        phaseChangedHandler,
                        () -> phaseChangedHandler.onPhaseChanged(newPhase),
                        "phaseChangedHandler");
            });
        } else {
            runHandler(
                    phaseChangedHandler,
                    () -> phaseChangedHandler.onPhaseChanged(newPhase),
                    "phaseChangedHandler");
        }

        return true;
    }

    @Override
    public boolean isRunning() {
        synchronized (phaseMonitor) {
            return phase == KafkaConsumerPhase.RUNNING || phase == KafkaConsumerPhase.PAUSED;
        }
    }

    /**
     * Ждать события Остановки или Паузы в течение таймаута
     *
     * @return 0 - произошло событие Остановки,
     * 1 - произошло событие Пауза,
     * WaitHandle.WAIT_TIMEOUT - истекло время ожидания, ни одного события не произошно
     */
    int waitForStopOrPause(long timeout) {
        return WaitHandle.waitAny(timeout, TimeUnit.MILLISECONDS, stoppedEvent, pausedEvent);
    }

    /**
     * Взведено ли событие Остановка
     */
    boolean isPaused() {
        return pausedEvent.isSet();
    }

    /**
     * Ждет (бесконечно) события Возобновления или события Остановки работы.
     *
     * @return true - дождались события Возобновление работы,
     * false - дождались события Остановки работы.
     */
    boolean waitForResumeOrStop() {
        return WaitHandle.waitAny(resumedEvent, stoppedEvent) == 0;
    }

    //endregion

    //region Playback time tracking

    @Setter
    protected PlaybackTimeHandler playbackTimeHandler;
    /**
     * Последняя зафиксированная временная метка воспроизведения,
     * которую получили в обработчике соотв. события от потребителя
     */
    private long playbackLastTs;
    /**
     * Периодичность трекинга времени воспроизведения
     */
    private volatile long playbackPeriod = 1000;
    /**
     * Дельта, на которую перемещается playbackLastTs
     */
    private volatile long playbackTsDelta;
    /**
     * Поток, который выполняет трекинг
     */
    private Thread playbackThread;

    private void initPlaybackTimeTracking() {
        if (consumingParams.getRegime() != ConsumingRegime.PAST_TIME_INTERVAL)
            // playback time tracking works for PAST_TIME_INTERVAL only
            return;

        if (playbackTimeHandler == null)
            return;

        // in case of a resuming after a pause, we should not initialize the playbackThread
        // so we initialize it only if the playbackThread is null
        if (playbackThread == null) {
            playbackLastTs = consumingParams.getFromTs();
            adjustPlaybackTimeTrackingParams(consumingParams.getRate());

            playbackThread = new Thread(this::PlaybackTimeTracking, "playback-time-tracking");
            playbackThread.start();
        }
    }

    /**
     * Подгоняет параметры трекинга (период трекинга и дельту перемещения временной метки)
     * в зависимости от скорости потребителя
     */
    private void adjustPlaybackTimeTrackingParams(double rate) {
        // если коэф. скорости меньше, чем "в 2 раза быстрее, чем реальная", то период = 1 сек,
        // если коэф. скорости от 2-х до 4-х, то период = 1/2 сек
        // иначе (еще быстрее), то период = 1/4 сек
        if (rate < 2)
            playbackPeriod = 1000;
        else if (2 <= rate && rate <= 4)
            playbackPeriod = 500;
        else
            playbackPeriod = 250;

        playbackTsDelta = (long) (playbackPeriod * rate);

        logger.debug("PlaybackTimeTracking params adjusted: playbackPeriod={}; playbackTsDelta={}",
                playbackPeriod, playbackTsDelta);
    }

    private void shutdownPlaybackTimeTracking() {
        if (playbackThread == null)
            return;

        try {
            playbackThread.join(100);
            playbackThread.interrupt();
        } catch (SecurityException | InterruptedException ignore) {
        }

        playbackThread = null;
    }

    private void PlaybackTimeTracking() {
        // todo: этот подход не идеален - если в процессе воспроизведения менять скорость (ускорять, замедлять),
        //  то время трекинга может начать отставать или опережать воспроизводимое время.
        //  чтобы это побороть, можно, например, периодически корректировать playbackLastTs, устанавливая его
        //  равным значению актуального времени воспроизведения.
        //  только нужно учитывать, при использовании KafkaConsumerService-а трекинг мы выполняем
        //  в экземпляре KafkaMultipleConsumer, а потребление данных из топиков - в KafkaSingleConsumer-ах

        while (true) {
            int eventIndex = waitForStopOrPause(playbackPeriod);
            if (eventIndex == 0) {
                logger.debug("consumer was stopped -> interrupt playback time tracking");
                return;
            } else if (eventIndex == 1) {
                logger.debug("consumer was paused -> sleep playback on until resume or final stop");

                if (waitForResumeOrStop()) {
                    logger.debug("consumer was resumed -> continue playback time tracking");
                } else {
                    logger.debug("consumer was stopped after pause -> interrupt playback time tracking");
                    return;
                }
            }
            // else - timeout

            long timestamp = playbackLastTs;
            // increase last timestamp
            playbackLastTs += playbackTsDelta;

            // todo: что если обработка потребовала много времени?
            runHandler(
                    playbackTimeHandler,
                    () -> playbackTimeHandler.onPlaybackTimeChanged(timestamp),
                    "playbackTimeHandler");
        }
    }

    //endregion

    //region run handler
    private void runHandler(Object handler, Runnable handlerInvoker, String handlerName) {
        runHandler(handler, handlerInvoker, error -> onError(handlerName + " failed", error));
    }

    private void runHandler(Object handler, Runnable handlerInvoker, ErrorHandler errorHandler) {
        if (handler == null)
            return;

        try {
            handlerInvoker.run();
        } catch (Throwable e) {
            logger.error("Handler failed: " + handler.getClass(), e);
            if (errorHandler != null)
                errorHandler.onError(e);
        }
    }
    //endregion
}
