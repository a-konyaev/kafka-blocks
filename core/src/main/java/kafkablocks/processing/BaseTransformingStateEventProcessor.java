package kafkablocks.processing;

import lombok.Getter;
import org.apache.kafka.streams.KeyValue;
import org.springframework.core.GenericTypeResolver;
import org.springframework.util.CollectionUtils;
import kafkablocks.events.Event;

import java.util.List;
import java.util.Objects;

/**
 * Базовый класс процессора-трансформера с состоянием,
 * который в результате обработки входного события формирует результирующее событие
 *
 * @param <EventToProcess>
 * @param <ResultEvent>
 * @param <ProcessorState>
 */
public abstract class BaseTransformingStateEventProcessor
        <EventToProcess extends Event, ResultEvent extends Event, ProcessorState extends EventProcessorState>
        extends BaseStateEventProcessor<EventToProcess, ProcessorState>
        implements
        TransformingEventProcessor<EventToProcess, ResultEvent> {

    @Getter
    private final Class<ResultEvent> resultEventType;


    @SuppressWarnings("unchecked")
    protected BaseTransformingStateEventProcessor() {
        super();

        Class<?>[] types = GenericTypeResolver.resolveTypeArguments(getClass(), BaseTransformingStateEventProcessor.class);
        resultEventType = (Class<ResultEvent>) Objects.requireNonNull(types, "Cannot resolve result event type")[1];
    }

    /**
     * Обработка сообщений
     */
    @Override
    public KeyValue<String, ResultEvent> transform(String key, EventToProcess eventToProcess) {
        logger.info("input event: {}", key);
        logger.debug("process input event: {} -> {}", key, eventToProcess);

        ProcessorState state;
        try {
            state = getState(key);
        } catch (RuntimeException e) {
            logger.error("getting state failed", e);
            return null;
        }

        List<ResultEvent> resList;
        try {
            resList = process(key, eventToProcess, state);
        } catch (Exception e) {
            logger.error("Event processing failed", e);
            return null;
        }

        if (CollectionUtils.isEmpty(resList)) {
            logger.debug("processing result is empty");
            return null;
        }

        resList.forEach(this::sendResultEvent);
        return null;
    }

    /**
     * Метод обработки текущего события из входного топика.
     *
     * @param key            ключ события
     * @param eventToProcess событие, которое нужно обработать
     * @param state          состояние, которое хранится для ключа события. Если состояние еще не определено, то null.
     * @return список результирующих событий или null, если результат обработки не требует выдавать выходные события
     */
    protected abstract List<ResultEvent> process(String key, EventToProcess eventToProcess, ProcessorState state);

    /**
     * Отправить результирующее событие в выходной топик
     *
     * @param resultEvent событие для отправки
     */
    protected void sendResultEvent(ResultEvent resultEvent) {
        logger.info("output event: {}", resultEvent.getKey());
        logger.debug("forward result event: {} -> {}", resultEvent.getKey(), resultEvent);
        this.context.forward(resultEvent.getKey(), resultEvent);
    }

    @Override
    public void close() {
    }
}
