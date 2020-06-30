package kafkablocks.utils;

public final class MathUtils {
    private MathUtils() {
    }

    /**
     * Получить номер позиции минимального (первого) установленного бита.
     * Примеры:
     * 36 = 100100 -> 3
     * 40 = 101000 -> 4
     * 41 = 101001 -> 1
     * 0  = 000000 -> 0
     */
    public static int getMinSetBitPosition(int value) {
        if (value == 0)
            return 0;

        int i = 1;
        int pos = 1;

        while ((i & value) == 0) {
            // Unset current bit and set the next bit in 'i'
            i = i << 1;
            // increment position
            ++pos;
        }

        return pos;
    }

    /**
     * Вычислить процент - сколько part составляет процентов от total
     *
     * @return % или 0, если part или total <= 0
     */
    public static int calcPercent(int part, int total) {
        if (part <= 0 || total <= 0)
            return 0;

        return (int) Math.round(((double) part / total) * 100);
    }
}
