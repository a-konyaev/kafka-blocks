package kafkablocks.examples.events;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class PositionEvent extends ObjectEvent {

    private int x;
    private int y;

    public PositionEvent(String objectId, int x, int y) {
        super(objectId);
        this.x = x;
        this.y = y;
    }
}
