package org.kafkablocks.consumer;

import org.kafkablocks.events.Event;


/**
 * Интерфейс для получения событий из Кафки, т.е. Потребителя
 */
public interface KafkaConsumer {
    /**
     * Идентификатор экземпляра потребителя
     */
    String getId();

    //region Lifecycle management

    void start();

    void stop();

    /**
     * Запущен ли сейчас потребитель.
     * @return true, если текущая фаза IN (RUNNING, PAUSED), иначе - false
     */
    boolean isRunning();

    /**
     * Приостановить работу.
     * Применимо только для режима PAST_TIME_INTERVAL
     */
    void pause();

    /**
     * Возобновить работу.
     * Применимо только для режима PAST_TIME_INTERVAL
     */
    void resume();

    KafkaConsumerPhase getPhase();

    //endregion

    //region Lifecycle events handling

    /**
     * Установить обработчик события "Изменение фазы"
     */
    void setPhaseChangedHandler(PhaseChangedHandler handler);

    /**
     * Установить обработчик ошибок
     */
    void setErrorHandler(ErrorHandler errorHandler);

    //endregion

    //region Consuming

    /**
     * Задать параметры получения событий из Кафки.
     * Метод должен быть вызван до запуска потребителя (start)
     */
    void setConsumingParams(ConsumingParams params);

    /**
     * Изменить скороть потребления
     * Применимо только для режима PAST_TIME_INTERVAL
     */
    void changeRate(double rate);

    /**
     * Установить интервал (в секундах) для срабатывания события "Простой", которое возникает,
     * если дольше, чем sec данный потребитель не получает события из кафки
     */
    void setIdleEventInterval(int sec);

    /**
     * Установить обработчик события "Простой"
     */
    void setIdleHandler(Runnable idleHandler);

    /**
     * Установить обработчик события "Время воспроизведения (изменилось)"
     * Применимо только для режима PAST_TIME_INTERVAL
     */
    void setPlaybackTimeHandler(PlaybackTimeHandler playbackTimeHandler);

    /**
     * Установить обработчик события, которое данный потребитель получает из топика Кафки.
     * Топик определяется по типу события.
     * Обработчик вызывается с параметрами:
     * - ключ события
     * - десериализованный экземпляр события
     */
    <T extends Event> void setEventHandler(EventHandler<T> eventHandler, Class<T> eventType);

    //endregion

    //region Filtering

    /**
     * Добавить фильтр, чтобы получать не все события, а только те, которые удовлетворяют фильтру.
     * Например, только события с заданным ключом = ИД объекта
     */
    void addFilter(Filter filter);

    /**
     * Удалить все фильтры
     */
    void resetFilters();

    //endregion
}
