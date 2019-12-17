package kafkablocks.examples.events;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class AccelerationEvent extends ObjectEvent {

    private double acceleration;

    public AccelerationEvent(String objectId, double acceleration) {
        super(objectId);
        this.acceleration = acceleration;
    }
}
