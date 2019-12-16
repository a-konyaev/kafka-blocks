package org.kafkablocks.examples.events;

import org.kafkablocks.events.BaseEvent;

public class AEvent extends BaseEvent {
    @Override
    public String getKey() {
        return null;
    }
}
