package kafkablocks.examples.events;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import kafkablocks.events.BaseEvent;

@NoArgsConstructor
@AllArgsConstructor
public abstract class ObjectEvent extends BaseEvent {

    private String objectId;

    @Override
    public String getKey() {
        return objectId;
    }
}
