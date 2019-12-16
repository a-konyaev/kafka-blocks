package org.kafkablocks.examples.events;

import org.kafkablocks.events.BaseEvent;

public class BEvent extends BaseEvent {
    @Override
    public String getKey() {
        return null;
    }
}
