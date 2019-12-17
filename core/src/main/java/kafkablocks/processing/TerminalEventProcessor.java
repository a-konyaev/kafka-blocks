package kafkablocks.processing;

import org.apache.kafka.streams.processor.Processor;
import kafkablocks.events.Event;


public interface TerminalEventProcessor
        <EventToProcess extends Event>
        extends
        EventProcessor<EventToProcess>,
        Processor<String, EventToProcess> {
}
