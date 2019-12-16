package org.kafkablocks.processing;

import org.apache.kafka.streams.processor.Processor;
import org.kafkablocks.events.Event;


public interface TerminalEventProcessor
        <EventToProcess extends Event>
        extends
        EventProcessor<EventToProcess>,
        Processor<String, EventToProcess> {
}
