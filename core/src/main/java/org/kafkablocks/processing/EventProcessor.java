package org.kafkablocks.processing;

import org.kafkablocks.events.Event;


public interface EventProcessor<EventToProcess extends Event> {

    Class<EventToProcess> getEventToProcessType();
}
