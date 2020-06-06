package kafkablocks.examples.events;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultipleObjectsPositionEventGenerator implements PositionEventGenerator {

    private final List<PositionEventGenerator> generators;
    private int pos = -1;

    public MultipleObjectsPositionEventGenerator(int objectCount) {
        generators = IntStream.range(1, objectCount + 1)
                .mapToObj(number -> new WalkingByCirclePositionEventGenerator(String.valueOf(number)))
                .collect(Collectors.toList());
    }

    @Override
    public PositionEvent getNextEvent() {
        pos = (++pos < generators.size()) ? pos : 0;
        return generators.get(pos).getNextEvent();
    }
}
