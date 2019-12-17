package kafkablocks.processing;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import kafkablocks.AppProperties;

import javax.validation.constraints.Positive;

/**
 * Настройки раннера процессоров
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Validated
@ConfigurationProperties(prefix = "kafkablocks.processing")
public class EventProcessorRunnerProperties extends AppProperties {
    /**
     * Кол-во тредов, которые будет использовать KafkaStreams
     */
    @Positive
    private int streamsThreadNumber = 1;
}
