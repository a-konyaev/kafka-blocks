package kafkablocks.examples.prioritizer;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import java.util.Random;

@UtilityClass
public class Constants {
    public final String LOW_PRIORITY_TOPIC = "low-priority";
    public final String HIGH_PRIORITY_TOPIC = "high-priority";

    public final long MESSAGE_PROCESSING_TIME = 30;

    public final long LOW_MESSAGE_PRODUCING_PERIOD = 200;

    public final long HIGH_MESSAGE_SLEEP_PERIOD = 10_000;
    public final long HIGH_MESSAGE_PRODUCING_PERIOD = 100;
    public final long HIGH_MESSAGES_BATCH_SIZE = 10;

    private final Random RANDOM = new Random();

    public long addNoise(long baseValue) {
        final double amplitude = 0.3;
        var from = baseValue * (1 - amplitude);
        var to = baseValue * (1 + amplitude);
        return Math.round(from + (RANDOM.nextDouble() * (to - from + 1)));
    }

    @SneakyThrows
    public void sleepAbout(long baseTimeout) {
        var timeout = addNoise(baseTimeout);
        Thread.sleep(timeout);
    }
}
