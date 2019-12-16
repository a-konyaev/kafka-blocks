package org.kafkablocks.events;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.UUID;

/**
 * Базовый интерфейс события
 */
public interface Event {
    /**
     * Уникальный идентификатор
     */
    UUID getId();

    /**
     * Временная метка события (т.е. когда оно было зафиксировано в системе вендора).
     */
    long getTimeStamp();

    /**
     * Ключ события, используется для отправки сообщений в Kafka.
     * У разных событий может быть одинаковый ключ (например, равный серийному номеру метки)
     * <p>
     * NOTE не использовать в бизнес логике (только для технических нужд)
     */
    @JsonIgnore
    String getKey();
}
