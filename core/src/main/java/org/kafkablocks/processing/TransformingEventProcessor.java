package org.kafkablocks.processing;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.kafkablocks.events.Event;


public interface TransformingEventProcessor
        <EventToProcess extends Event, ResultEvent extends Event>
        extends
        EventProcessor<EventToProcess>,
        Transformer<String, EventToProcess, KeyValue<String, ResultEvent>> {

    Class<ResultEvent> getResultEventType();
}
