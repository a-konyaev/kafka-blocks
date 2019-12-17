package kafkablocks.examples.events;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class SpeedEvent extends ObjectEvent {

    private double speed;

    public SpeedEvent(String objectId, double speed) {
        super(objectId);
        this.speed = speed;
    }
}
