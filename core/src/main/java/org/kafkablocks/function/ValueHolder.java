package org.kafkablocks.function;

import lombok.Data;

/**
 * Этот класс удобно использовать, когда нужно изменять переменную из асинхронно-запущенной лямбды
 * @param <T>
 */
@Data
public final class ValueHolder<T> {
    private T value;

    public ValueHolder(T value) {
        this.value = value;
    }
}
