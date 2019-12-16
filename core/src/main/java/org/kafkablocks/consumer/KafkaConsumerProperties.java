package org.kafkablocks.consumer;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.kafkablocks.AppProperties;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


/**
 * Параметры Потребителя
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Validated
@ConfigurationProperties(prefix = "org.kafkablocks.consumer")
public class KafkaConsumerProperties extends AppProperties {
    /**
     * Левая граница интервала "С" при типе потребления PAST_TIME_INTERVAL
     */
    private String from;
    /**
     * Правая граница интервала "С" при типе потребления PAST_TIME_INTERVAL
     */
    private String to;
    /**
     * Коэффициент скорости чтения событий при типе потребления PAST_TIME_INTERVAL.
     * По умолчанию, 1.0 - т.е. скорость чтения будет соответствовать той, с которой события поступали в топик.
     */
    private double rate = 1.0;

    /**
     * Параметры потребления
     */
    private ConsumingParams consumingParams;


    @Override
    protected void init() {
        if (StringUtils.isEmpty(from)) {
            consumingParams = ConsumingParamsBuilder.realtime();
            return;
        }

        try {
            ConsumingParamsBuilder builder = new ConsumingParamsBuilder(ConsumingRegime.PAST_TIME_INTERVAL)
                    .from(parseDateTime(from));

            if (!StringUtils.isEmpty(to))
                builder.to(parseDateTime(to));

            consumingParams = builder.withRate(rate).build();

        } catch (Exception e) {
            throw new RuntimeException("Building ConsumingParams failed", e);
        }
    }

    private static LocalDateTime parseDateTime(String value) {
        final String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimeFormat);

        try {
            return LocalDateTime.parse(value, formatter);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Error while parsing DateTime from '%s'. Value must have format '%s'",
                            value, dateTimeFormat),
                    e);
        }
    }
}
