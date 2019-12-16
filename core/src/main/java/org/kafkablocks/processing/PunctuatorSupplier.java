package org.kafkablocks.processing;

import lombok.Getter;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;


public class PunctuatorSupplier {
    @Getter
    private final Punctuator punctuator;
    @Getter
    private final long intervalMs;
    @Getter
    private PunctuationType punctuationType = PunctuationType.WALL_CLOCK_TIME;


    // TODO: не работают валидаторы... тк видимо это не бин. надо потом разобраться.
    public PunctuatorSupplier(@NotNull Punctuator punctuator, @Positive long intervalMs) {
        this.punctuator = punctuator;
        this.intervalMs = intervalMs;
    }
}
