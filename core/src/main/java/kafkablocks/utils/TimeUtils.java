package kafkablocks.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

/**
 * Работа с датами и временем
 */
public final class TimeUtils {
    private TimeUtils() {
    }

    /**
     * System default zone.
     */
    public static ZoneId DEFAULT_ZONE = ZoneId.systemDefault();

    /**
     * Смещение текущего часового пояса
     */
    public static final ZoneOffset LOCAL_ZONE_OFFSET = DEFAULT_ZONE.getRules().getOffset(LocalDateTime.now());


    /**
     * Привести время к таймстампу. Используется текущий часовой пояс.
     */
    public static long toTimestamp(LocalDateTime dateTime) {
        return dateTime.toInstant(LOCAL_ZONE_OFFSET).toEpochMilli();
    }

    public static long getNowTimestamp() {
        return toTimestamp(LocalDateTime.now());
    }

    /**
     * Convert timestamp to date time at system default zone.
     *
     * @param timestamp value accurate to milliseconds
     */
    public static LocalDateTime fromTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), DEFAULT_ZONE);
    }

    public static OffsetDateTime offsetDateTimeFromTimestamp(long timestamp, ZoneId zone) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zone);
    }

    /**
     * Convert date time in any zone to date time at system default zone.
     *
     * @param zonedDateTime date time in any zone
     * @see TimeUtils#DEFAULT_ZONE
     */
    public static LocalDateTime fromZonedDateTime(ZonedDateTime zonedDateTime) {
        return Optional.ofNullable(zonedDateTime)
                .map(zdt -> zdt.withZoneSameInstant(DEFAULT_ZONE))
                .map(ZonedDateTime::toLocalDateTime).orElse(null);
    }

    /**
     * ZonedDateTime в OffsetDateTime.
     * @param zonedDateTime ZonedDateTime
     * @return OffsetDateTime
     */
    public static OffsetDateTime zonedDateTimeToOffsetDateTime(ZonedDateTime zonedDateTime){
        return Optional.ofNullable(zonedDateTime)
                .map(zdt -> zdt.withZoneSameInstant(DEFAULT_ZONE))
                .map(ZonedDateTime::toOffsetDateTime).orElse(null);
    }
}
