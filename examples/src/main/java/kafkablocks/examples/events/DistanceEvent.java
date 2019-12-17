package kafkablocks.examples.events;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class DistanceEvent extends ObjectEvent {

    private int distance;

    public DistanceEvent(String objectId, int distance) {
        super(objectId);
        this.distance = distance;
    }
}
