package kafkablocks.processing;

import kafkablocks.events.Event;

/**
 * Базовый процессор, который обрабатывает стрим
 * @param <EventToProcess> тип события, которое является "значением" в паре ключ-значение стрима
 */
public abstract class BaseStreamProcessor
        <EventToProcess extends Event>
        extends BaseEventProcessor<EventToProcess>
        implements
        StreamProcessor<EventToProcess> {
}
