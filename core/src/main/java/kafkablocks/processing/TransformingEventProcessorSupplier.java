package kafkablocks.processing;

import org.apache.kafka.streams.kstream.TransformerSupplier;
import kafkablocks.events.Event;

@FunctionalInterface
public interface TransformingEventProcessorSupplier<V extends Event, R extends Event> {

    TransformerSupplier<String, V, R> get(Class<? extends TransformingEventProcessor<? extends Event, ? extends Event>> processorClass);

}
