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
@ToString
public abstract class BaseEvent implements Event {
    /**
     * Уникальный идентификатор события
     */
    private final UUID id = UUID.randomUUID();
    /**
     * Время наступления события
     * По умолчанию - текущее время в дефолтной временной зоне
     */
    private OffsetDateTime occurred = OffsetDateTime.now();
}
