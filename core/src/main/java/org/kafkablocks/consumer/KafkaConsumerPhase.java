package org.kafkablocks.consumer;

import org.kafkablocks.utils.TextUtils;

import java.util.Arrays;

/**
 * Фазы работы потребителя.
 * Жизненный цикл:
 * NOT_STARTED -> RUNNING
 * RUNNING -> PAUSED | STOPPED
 * PAUSED -> RUNNING | STOPPED
 *
 * todo: для тестов нужно уметь перезапускать после остановки, но правильно ли?
 * STOPPED -> RUNNING
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
     * @param nextPhase следующая фаза
     * @return следующая фаза
     */
    public KafkaConsumerPhase assertNextPhase(KafkaConsumerPhase nextPhase) {
        switch (this) {
            case NOT_STARTED:
            case STOPPED:
                assertNextPhase(nextPhase, KafkaConsumerPhase.RUNNING);
                break;

            case RUNNING:
                assertNextPhase(
                        nextPhase,
                        KafkaConsumerPhase.PAUSED, KafkaConsumerPhase.STOPPED);
                break;

            case PAUSED:
                assertNextPhase(nextPhase, KafkaConsumerPhase.RUNNING, KafkaConsumerPhase.STOPPED);
                break;
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
