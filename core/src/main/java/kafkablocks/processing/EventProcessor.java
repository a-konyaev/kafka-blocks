package kafkablocks.processing;

import kafkablocks.events.Event;


public interface EventProcessor<EventToProcess extends Event> {

    Class<EventToProcess> getEventToProcessType();
}
