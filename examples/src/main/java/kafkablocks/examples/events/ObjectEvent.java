package kafkablocks.examples.events;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import kafkablocks.events.BaseEvent;
import lombok.ToString;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
public abstract class ObjectEvent extends BaseEvent {

    private String objectId;

    @Override
    public String getKey() {
        return objectId;
    }
}
