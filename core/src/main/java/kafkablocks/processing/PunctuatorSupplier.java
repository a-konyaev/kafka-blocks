package kafkablocks.processing;

import lombok.Getter;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.springframework.lang.NonNull;

import java.time.Duration;

@Getter
public class PunctuatorSupplier {

    private final Punctuator punctuator;
    private final Duration interval;
    private PunctuationType punctuationType = PunctuationType.WALL_CLOCK_TIME;

    public PunctuatorSupplier(@NonNull Punctuator punctuator, @NonNull Duration interval) {
        this.punctuator = punctuator;
        this.interval = interval;
    }
}
