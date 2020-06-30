package kafkablocks.processing;

import org.apache.kafka.streams.kstream.KStream;
import kafkablocks.events.Event;

public interface StreamProcessor
        <EventToProcess extends Event>
        extends
        EventProcessor<EventToProcess> {

    KStream<String, EventToProcess> process(KStream<String, EventToProcess> stream);

}
