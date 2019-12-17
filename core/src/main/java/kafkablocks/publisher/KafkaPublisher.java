package kafkablocks.publisher;

import org.slf4j.Logger;
import kafkablocks.events.Event;


/**
 * Интерфейс для публикации событий в кафку
 */
public interface KafkaPublisher {
    /**
     * Задать логгер
     */
    void setLogger(Logger logger);

    /**
     * Опубликовать событие
     */
    void publishEvent(Event event);
}
