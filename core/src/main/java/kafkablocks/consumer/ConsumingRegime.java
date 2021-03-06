package kafkablocks.consumer;

/**
 * Режим работы потребителя
 */
public enum ConsumingRegime {
    /**
     * В реальном времени.
     * Получать только новые события.
     */
    REAL_TIME,
    /**
     * Интервал в прошлом.
     * Получать события, которые поступили в топик ранее, в течение интервала [С; ПО],
     * где границы интервала задаются параметрами from и to.
     */
    PAST_TIME_INTERVAL
}
