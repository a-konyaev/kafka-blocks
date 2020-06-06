package kafkablocks.events;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Базовый интерфейс события
 */
public interface Event {
    /**
     * Уникальный идентификатор
     */
    String getId();

    /**
     * Время регистрации события
     */
    OffsetDateTime getOccurred();

    /**
     * Ключ события, используется для отправки сообщений в Kafka.
     * У разных событий может быть одинаковый ключ
     * <p>
     * NOTE не использовать в бизнес логике - только для технических нужд
     */
    @JsonIgnore
    String getKey();
}
