package kafkablocks.processing;

import kafkablocks.AppProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Positive;

/**
 * Настройки раннера процессоров
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Validated
@ConfigurationProperties(prefix = "whswd.kafka.processing")
public class EventProcessorRunnerProperties extends AppProperties {
    /**
     * Кол-во тредов, которые будет использовать KafkaStreams
     */
    @Positive
    private int streamsThreadNumber = 1;
}
