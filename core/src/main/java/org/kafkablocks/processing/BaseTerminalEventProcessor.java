package org.kafkablocks.processing;

import org.kafkablocks.events.Event;


/**
 * Базовый класс терминального процессора без состояния
 * (терминальный - т.е. не порождает событий в результате обработки входных событий)
 *
 * @param <EventToProcess>
 */
public abstract class BaseTerminalEventProcessor
        <EventToProcess extends Event>
        extends BaseEventProcessor<EventToProcess>
        implements
        TerminalEventProcessor<EventToProcess> {

    @Override
    protected void init() {
        // do nothing
    }

    @Override
    public void process(String key, EventToProcess value) {
        try {
            processEvent(key, value);
        } catch (Exception e) {
            logger.error("Event processing failed", e);
        }
    }

    /**
     * Метод обработки текущего события из входного топика.
     *
     * @param key            ключ события
     * @param eventToProcess событие, которое нужно обработать
     */
    protected abstract void processEvent(String key, EventToProcess eventToProcess);

    @Override
    public void close() {
    }
}
