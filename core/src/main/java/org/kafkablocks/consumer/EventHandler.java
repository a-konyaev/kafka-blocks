package org.kafkablocks.consumer;

import org.kafkablocks.events.Event;


/**
 * Интерфейс обработчика событий
 * @param <T>
 */
public interface EventHandler<T extends Event> {
    /**
     * Обработать событие
     * @param key ключ события
     * @param event экземпляр события
     */
    void process(String key, T event);
}
