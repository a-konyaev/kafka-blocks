package kafkablocks.examples.events;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@NoArgsConstructor
@ToString
public class DistanceEvent extends ObjectEvent {

    private int distance;

    public DistanceEvent(String objectId, int distance) {
        super(objectId);
        this.distance = distance;
    }
}
