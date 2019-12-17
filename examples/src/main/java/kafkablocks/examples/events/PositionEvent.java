package kafkablocks.examples.events;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class PositionEvent extends ObjectEvent {

    private int x;
    private int y;

}
