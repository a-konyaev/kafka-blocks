package kafkablocks.events;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Базовый класс для всех событий
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
public abstract class BaseEvent implements Event {
    /**
     * Уникальный идентификатор события
     */
    private final String id = UUID.randomUUID().toString();
    /**
     * Время наступления события
     * По умолчанию - текущее время в дефолтной временной зоне
     */
    private OffsetDateTime occurred = OffsetDateTime.now();
}
