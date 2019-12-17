package kafkablocks.processing;

import org.apache.kafka.streams.state.StoreBuilder;
import kafkablocks.events.Event;


public interface StateEventProcessor
        <EventToProcess extends Event, ProcessorState extends EventProcessorState>
        extends EventProcessor<EventToProcess> {

    String getStateStoreName();

    StoreBuilder getStateStoreBuilder();
}
