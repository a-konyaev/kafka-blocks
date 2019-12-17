package kafkablocks.consumer;

/**
 * Интерфейс Обработчика изменения фазы работы потребителя
 */
public interface PhaseChangedHandler {
    /**
     * Обработчика изменения фазы работы потребителя
     * @param phase новая фаза
     */
    void onPhaseChanged(KafkaConsumerPhase phase);
}
