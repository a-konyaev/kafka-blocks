package kafkablocks.processing;

import kafkablocks.events.Event;

public abstract class BaseTerminalStateEventProcessor
        <EventToProcess extends Event, ProcessorState extends EventProcessorState>
        extends BaseStateEventProcessor<EventToProcess, ProcessorState>
        implements
        TerminalEventProcessor<EventToProcess> {

    @Override
    public void process(String key, EventToProcess value) {
        logger.info("input event: {}", key);
        logger.debug("process input event: {} -> {}", key, value);

        ProcessorState state;
        try {
            state = getState(key);
        } catch (RuntimeException e) {
            logger.error("getting state failed", e);
            return;
        }

        try {
            process(key, value, state);
        } catch (Exception e) {
            logger.error("Event processing failed", e);
        }
    }

    protected abstract void process(String key, EventToProcess eventToProcess, ProcessorState state);

    @Override
    public void close() {
    }
}
