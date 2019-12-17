package kafkablocks.processing;

import org.apache.kafka.streams.kstream.TransformerSupplier;

@FunctionalInterface
public interface TransformingEventProcessorSupplier {
    TransformerSupplier get(Class<? extends TransformingEventProcessor> processorClass);
}
