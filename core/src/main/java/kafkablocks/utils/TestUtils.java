package kafkablocks.utils;

import ch.qos.logback.classic.Level;
import lombok.SneakyThrows;
import org.slf4j.LoggerFactory;

public final class TestUtils {
    private TestUtils() {
    }

    @SneakyThrows
    public static void sleepRandom(int millisFrom, int millisTo) {
        int delay = RandomUtils.getRandomInt(millisFrom, millisTo);
        Thread.sleep(delay);
    }

    public static void setRootLoggerLevel(String levelName) {
        setLoggerLevel(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME, levelName);
    }

    public static void setLoggerLevel(String loggerName, String levelName) {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(loggerName);
        logger.setLevel(Level.toLevel(levelName));
    }
}
