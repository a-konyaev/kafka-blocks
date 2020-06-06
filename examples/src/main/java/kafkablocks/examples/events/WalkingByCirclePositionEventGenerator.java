package kafkablocks.examples.events;

public class WalkingByCirclePositionEventGenerator implements PositionEventGenerator {
    public static final double RADIUS = 10;
    public static final double SPEED = 1.0;

    private final String objectId;
    private int step = 0;
    private int index = 0;

    public WalkingByCirclePositionEventGenerator() {
        this(String.valueOf(System.currentTimeMillis()));
    }

    public WalkingByCirclePositionEventGenerator(String objectId) {
        this.objectId = objectId;
    }

    @Override
    public PositionEvent getNextEvent() {
        double radians = Math.PI / 180 * (++step) * SPEED;
        int x = (int) Math.round(RADIUS * Math.sin(radians));
        int y = (int) Math.round(RADIUS * Math.cos(radians));
        return new PositionEvent(objectId, x, y);
    }
}
