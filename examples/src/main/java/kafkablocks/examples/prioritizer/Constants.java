package kafkablocks.examples.prioritizer;

import lombok.SneakyThrows;

import java.util.Random;

public final class Constants {
    public static final String LOW_PRIORITY_TOPIC = "low-priority";
    public static final String HIGH_PRIORITY_TOPIC = "high-priority";

    public static final long CHANNEL_SEND_TIMEOUT = 50;
    public static final long CHANNEL_RECEIVE_TIMEOUT = 10;

    public static final long LOW_MESSAGE_PRODUCING_PERIOD = 20;

    public static final long HIGH_MESSAGE_SLEEP_PERIOD = 1_000;
    public static final long HIGH_MESSAGE_PRODUCING_PERIOD = 10;
    public static final long HIGH_MESSAGES_BATCH_SIZE = 10;

    public static final long MESSAGE_PROCESSING_TIME = 10;
    public static final int MAX_LOW_MESSAGES = 5_000;

    private static final Random RANDOM = new Random();

    public static long addNoise(long baseValue) {
        final double amplitude = 0.3;
        var from = baseValue * (1 - amplitude);
        var to = baseValue * (1 + amplitude);
        return Math.round(from + (RANDOM.nextDouble() * (to - from + 1)));
    }

    @SneakyThrows
    public static void sleepAbout(long baseTimeout) {
        var timeout = addNoise(baseTimeout);
        Thread.sleep(timeout);
    }
}
