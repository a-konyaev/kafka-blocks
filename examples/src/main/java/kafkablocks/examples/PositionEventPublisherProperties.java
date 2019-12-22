package kafkablocks.examples;

import kafkablocks.AppProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

@Data
@EqualsAndHashCode(callSuper = true)
@Validated
@ConfigurationProperties(prefix = "kafkablocks.examples")
public class PositionEventPublisherProperties extends AppProperties {

    /**
     * Кол-во параллельно запущенных генераторов событий
     */
    @PositiveOrZero
    private int publisherCount = 0;
}
