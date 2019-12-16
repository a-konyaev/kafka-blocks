package org.kafkablocks.consumer;

import lombok.Getter;

import java.time.LocalDateTime;


/**
 * Параметры потребления
 */
@Getter
public class ConsumingParams {
    private final ConsumingRegime regime;
    private final LocalDateTime from;
    private final long fromTs;
    private final LocalDateTime to;
    private final long toTs;

    public static double RATE_MIN = 0.001;
    public static double RATE_MAX = 1000;

    // todo: не оч. клевое решение - по хорошему этот класс не должен менять значение своих полей.
    //  но иначе придется в консьюмере иметь отдельное поле rate, что тоже не айс.
    private volatile double rate;

    void setRate(double rate) {
        ConsumingParamsBuilder.assertRate(rate);
        this.rate = rate;
    }

    ConsumingParams(ConsumingRegime regime,
                    LocalDateTime from,
                    long fromTs,
                    LocalDateTime to,
                    long toTs,
                    double rate) {
        this.regime = regime;
        this.from = from;
        this.fromTs = fromTs;
        this.to = to;
        this.toTs = toTs;
        this.rate = rate;
    }
}
