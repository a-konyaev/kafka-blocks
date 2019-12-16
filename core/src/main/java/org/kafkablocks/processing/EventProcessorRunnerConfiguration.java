package org.kafkablocks.processing;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.kafkablocks.EventTopicProperties;

import java.util.List;

/**
 * @see EnableEventProcessorRunner
 */
@Configuration
@Import({EventProcessorRunnerProperties.class})
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class EventProcessorRunnerConfiguration {

    private final ApplicationContext appContext;
    private final EventProcessorRunnerProperties runnerProperties;
    private final EventTopicProperties eventTopicProperties;
    private final KafkaProperties kafkaProperties;
    private final List<EventProcessor> processors;

    @Bean
    public EventProcessorRunner eventProcessorRunner() {
        return new EventProcessorRunner(appContext,
            runnerProperties,
            eventTopicProperties,
            kafkaProperties,
            processors);
    }

}
