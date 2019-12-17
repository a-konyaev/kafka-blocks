package kafkablocks.examples.events;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class SpeedEvent extends ObjectEvent {

    private double speed;

}
