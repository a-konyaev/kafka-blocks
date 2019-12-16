package org.kafkablocks.function;


/**
 * Интерфейс обработки ошибок
 */
public interface ErrorHandler<T> {
    /**
     * Метод обработчик ошибки
     * @param source объект-источник ошибки, т.е. где ошибка была обнаружена
     * @param error экземпляр исключения, которое стало причиной вызова данного обработчика
     */
    void onError(T source, Throwable error);
}
