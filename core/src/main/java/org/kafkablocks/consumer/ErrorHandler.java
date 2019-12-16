package org.kafkablocks.consumer;


/**
 * Интерфейс обработки ошибок
 */
public interface ErrorHandler {
    /**
     * Метод обработчик ошибки
     * @param error экземпляр исключения, которое стало причиной вызова данного обработчика
     */
    void onError(Throwable error);
}
