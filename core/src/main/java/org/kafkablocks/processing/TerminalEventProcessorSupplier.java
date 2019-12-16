package org.kafkablocks.processing;

import org.apache.kafka.streams.processor.ProcessorSupplier;

@FunctionalInterface
public interface TerminalEventProcessorSupplier {
    ProcessorSupplier get(Class<? extends TerminalEventProcessor> processorClass);
}
