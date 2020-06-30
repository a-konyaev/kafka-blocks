package kafkablocks.processing;

import kafkablocks.EventTopicProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import kafkablocks.events.Event;

import java.util.List;

/**
 * @see EnableEventProcessorRunner
 */
@Configuration
@Import({EventProcessorRunnerProperties.class})
@RequiredArgsConstructor
public class EventProcessorRunnerConfiguration {

    private final ApplicationContext appContext;
    private final EventProcessorRunnerProperties runnerProperties;
    private final EventTopicProperties eventTopicProperties;
    private final KafkaProperties kafkaProperties;
    private final List<EventProcessor<? extends Event>> processors;

    @Bean
    public EventProcessorRunner eventProcessorRunner() {
        return new EventProcessorRunner(
                appContext,
                runnerProperties,
                eventTopicProperties,
                kafkaProperties,
                processors);
    }

}
