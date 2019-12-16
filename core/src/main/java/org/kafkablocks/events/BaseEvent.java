package org.kafkablocks.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Базовый класс для всех событий
 */
@Data
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
     */
    private LocalDateTime occurred = LocalDateTime.now();
    /**
     * ИД системы-источника события
     */
    private String sourceSystemId;


    public BaseEvent(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }

    private static final ZoneOffset LOCAL_ZONE_OFFSET = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());

    /**
     * Время наступления события в мсек
     */
    @Override
    public long getTimeStamp() {
        // todo: переделать так, чтобы не вычислять таймстамп при каждом обращении к методу
        return occurred.toInstant(LOCAL_ZONE_OFFSET).toEpochMilli();
    }
}
