package kafkablocks.examples.events;

public interface PositionEventGenerator {
    PositionEvent getNextEvent();
}
