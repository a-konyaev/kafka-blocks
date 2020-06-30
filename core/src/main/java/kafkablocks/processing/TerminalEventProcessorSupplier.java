package kafkablocks.processing;

import org.apache.kafka.streams.processor.ProcessorSupplier;
import kafkablocks.events.Event;

@FunctionalInterface
public interface TerminalEventProcessorSupplier<V extends Event> {

    ProcessorSupplier<String, V> get(Class<? extends TerminalEventProcessor<V>> processorClass);

}
