package kafkablocks.publisher;

import org.slf4j.Logger;
import kafkablocks.events.Event;

/**
 * Интерфейс для публикации событий в кафку
 */
public interface KafkaPublisher {
    /**
     * Задать логгер.
     */
    void setLogger(Logger logger);

    /**
     * Опубликовать событие.
     * Топик, в который будет опибликовано событие, вычисляется автоматически по классу события.
     */
    void publishEvent(Event event);

    /**
     * Опубликовать событие в заданный топик.
     */
    void publishEvent(Event event, String topic);
}
