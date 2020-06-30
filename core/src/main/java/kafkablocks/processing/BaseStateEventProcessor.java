package kafkablocks.processing;

import kafkablocks.events.Event;
import kafkablocks.serialization.SerdeProvider;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.core.GenericTypeResolver;
import org.springframework.lang.NonNull;

import java.util.Objects;

/**
 * Базовый процессор, у которого есть состояние.
 *
 * @param <EventToProcess> тип обрабатываемого события
 * @param <ProcessorState> тип состояния. Состояние создается для каждого уникального ключа обрабатываемого события
 */
public abstract class BaseStateEventProcessor
        <EventToProcess extends Event, ProcessorState extends EventProcessorState>
        extends BaseEventProcessor<EventToProcess>
        implements
        StateEventProcessor<EventToProcess, ProcessorState> {

    private final Class<ProcessorState> stateType;
    @Getter
    private final String stateStoreName;
    protected KeyValueStore<String, ProcessorState> stateStore;


    @SuppressWarnings("unchecked")
    BaseStateEventProcessor() {
        super();

        Class<?>[] types = GenericTypeResolver.resolveTypeArguments(getClass(), BaseStateEventProcessor.class);
        stateType = (Class<ProcessorState>) Objects.requireNonNull(types, "Cannot resolve state type")[1];

        stateStoreName = getClass().getName() + "_stateStore";
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void init() {
        stateStore = (KeyValueStore<String, ProcessorState>) context.getStateStore(this.stateStoreName);
    }

    @Override
    public StoreBuilder<? extends StateStore> getStateStoreBuilder() {
        return Stores
                .keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(stateStoreName),
                        Serdes.String(),
                        SerdeProvider.getSerde(stateType))
                .withCachingEnabled()
                .withLoggingDisabled();
    }

    /**
     * Получить состояние
     *
     * @return состояние или null, если состояние не было ранее сохранено
     */
    protected ProcessorState getState(@NonNull String key) {
        ProcessorState state = stateStore.get(key);

        // TODO: сделать реализацию с NotNull-состоянием, чтобы можно было проверять так: state.isEmpty()
        if (state == null) {
            logger.debug("state for key={} is empty", key);
            return null;
        }

        logger.debug("state for key={}: {}", key, state);
        return state;
    }

    /**
     * Обновить состояние
     */
    protected void updateState(@NonNull String key, @NonNull ProcessorState newState) {
        logger.debug("set new state: key={}; value={}", key, newState);
        newState.updateTimestamp();
        stateStore.put(key, newState);
    }

    /**
     * Сбросить состояние.
     * Состояние фактически удаляется, но если вызвать {@link this.getState()},
     * то будет возвращено пустое (не путать с null) состояние
     */
    protected void resetState(@NonNull String key) {
        logger.debug("reset state for key={}", key);
        stateStore.put(key, null);
    }
}
