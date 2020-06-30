package kafkablocks.consumer;

import kafkablocks.utils.TextUtils;

import java.util.Arrays;

/**
 * Фазы работы потребителя.
 * Жизненный цикл:
 * <pre>
 *  ( )         -> NOT_STARTED
 *  NOT_STARTED -> RUNNING
 *  RUNNING     -> PAUSED   |   STOPPED
 *  PAUSED      -> RUNNING  |   STOPPED
 *  STOPPED     -> (x)
 * </pre>
 */
public enum KafkaConsumerPhase {
    /**
     * Initial phase
     */
    NOT_STARTED,
    /**
     * After starting
     */
    RUNNING,
    /**
     * When was paused while running
     */
    PAUSED,
    /**
     * Stopped by the user or by itself, because all records have been consumed
     */
    STOPPED;


    /**
     * Проверить корректность перехода от текущей фазы (данного экземпляра) к следующей
     *
     * @param nextPhase следующая фаза
     * @return следующая фаза
     */
    public KafkaConsumerPhase assertNextPhase(KafkaConsumerPhase nextPhase) {
        switch (this) {
            case NOT_STARTED:
                assertNextPhase(nextPhase, KafkaConsumerPhase.RUNNING);
                break;

            case RUNNING:
                assertNextPhase(nextPhase, KafkaConsumerPhase.PAUSED, KafkaConsumerPhase.STOPPED);
                break;

            case PAUSED:
                assertNextPhase(nextPhase, KafkaConsumerPhase.RUNNING, KafkaConsumerPhase.STOPPED);
                break;

            case STOPPED:
                throw new IllegalStateException("STOPPED is a final phase. No available next phases.");
        }

        return nextPhase;
    }

    private void assertNextPhase(
            KafkaConsumerPhase actualNextPhase,
            KafkaConsumerPhase... expectedNextPhases) {

        if (Arrays.asList(expectedNextPhases).contains(actualNextPhase))
            return;

        throw new IllegalStateException(
                String.format("Change phase from %s to %s is unacceptable. Available next phases are: %s",
                        this, actualNextPhase, TextUtils.joinToString(expectedNextPhases)));
    }
}
