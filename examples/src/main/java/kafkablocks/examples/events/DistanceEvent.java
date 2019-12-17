package kafkablocks.examples.events;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class DistanceEvent extends ObjectEvent {

    private int distance;

}
