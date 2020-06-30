package kafkablocks.consumer;

import kafkablocks.utils.TimeUtils;
import org.springframework.util.Assert;
import java.time.LocalDateTime;

/**
 * Конструктор параметров потребителя
 */
public final class ConsumingParamsBuilder {
    private final ConsumingRegime regime;
    private LocalDateTime from;
    private LocalDateTime to;
    private double rate = 1.0;

    /**
     * Параметры для потребления в реальном времени
     */
    public static ConsumingParams realtime() {
        return new ConsumingParamsBuilder(ConsumingRegime.REAL_TIME).build();
    }

    /**
     * Создать конструктор параметров с заданным режимом потребления
     */
    public ConsumingParamsBuilder(ConsumingRegime regime) {
        this.regime = regime;
    }

    /**
     * Задать значение параметра "Дата С"
     */
    public ConsumingParamsBuilder from(LocalDateTime dateTime) {
        this.from = dateTime;
        return this;
    }

    /**
     * Задать значение параметра "Дата ПО"
     */
    public ConsumingParamsBuilder to(LocalDateTime dateTime) {
        this.to = dateTime;
        return this;
    }

    /**
     * Задать значение параметра "Скорость потребления"
     */
    public ConsumingParamsBuilder withRate(double rate) {
        assertRate(rate);
        this.rate = rate;
        return this;
    }

    static void assertRate(double rate) {
        Assert.isTrue(ConsumingParams.RATE_MIN <= rate && rate <= ConsumingParams.RATE_MAX,
                String.format("'rate' must be from %f to %f", ConsumingParams.RATE_MIN, ConsumingParams.RATE_MAX));
    }

    /**
     * Выполняет конструирование экземпляра параметров на основе значений,
     * которые были ранее заданы для конструктора
     */
    public ConsumingParams build() {
        if (regime == ConsumingRegime.REAL_TIME) {
            return new ConsumingParams(
                    ConsumingRegime.REAL_TIME,
                    null, 0,
                    null, Long.MAX_VALUE,
                    1.0);
        }

        Assert.notNull(from, "'from' must not be null");

        LocalDateTime now = LocalDateTime.now();

        if (to == null) {
            Assert.isTrue(from.isBefore(now), "'from' must be before now");

            return new ConsumingParams(
                    ConsumingRegime.PAST_TIME_INTERVAL,
                    from, TimeUtils.toTimestamp(from),
                    null, Long.MAX_VALUE,
                    rate);
        }
        // to != null

        Assert.isTrue(to.isBefore(now), "'to' must be before now");
        Assert.isTrue(from.isBefore(to), "'from' must be before 'to'");

        return new ConsumingParams(
                ConsumingRegime.PAST_TIME_INTERVAL,
                from, TimeUtils.toTimestamp(from),
                to, TimeUtils.toTimestamp(to),
                rate);
    }
}
