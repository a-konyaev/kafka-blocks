package kafkablocks.utils;

import java.util.Random;

public final class RandomUtils {
    private static final Random RANDOM = new Random();

    private RandomUtils() {
    }

    public static int getRandomInt(int from, int to) {
        return from + (int) (RANDOM.nextFloat() * (to - from + 1));
    }

    public static int getRandomIntWithAmplitude(int baseValue, int amplitude) {
        return getRandomInt(baseValue - amplitude, baseValue + amplitude);
    }

    public static double getRandomDouble(double from, double to) {
        return from + RANDOM.nextFloat() * (to - from);
    }

    public static double getRandomDoubleWithAmplitude(double baseValue, double amplitude) {
        return getRandomDouble(baseValue - amplitude, baseValue + amplitude);
    }
}
