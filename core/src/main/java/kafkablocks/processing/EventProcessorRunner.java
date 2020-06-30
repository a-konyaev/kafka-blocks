package kafkablocks.processing;

import kafkablocks.EventTopicProperties;
import kafkablocks.ServiceBase;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import kafkablocks.events.Event;

import java.util.List;
import java.util.Properties;

@EnableConfigurationProperties({
        EventProcessorRunnerProperties.class,
        EventTopicProperties.class,
        KafkaProperties.class})
public class EventProcessorRunner extends ServiceBase {

    private final EventProcessorRunnerProperties runnerProperties;
    private final EventTopicProperties eventTopicProperties;
    private final KafkaProperties kafkaProperties;
    private final List<EventProcessor<? extends Event>> processors;

    private KafkaStreams kafkaStreams;

    public EventProcessorRunner(ApplicationContext appContext,
                                EventProcessorRunnerProperties runnerProperties,
                                EventTopicProperties eventTopicProperties,
                                KafkaProperties kafkaProperties,
                                List<EventProcessor<? extends Event>> processors) {
        this.appContext = appContext;
        this.runnerProperties = runnerProperties;
        this.eventTopicProperties = eventTopicProperties;
        this.kafkaProperties = kafkaProperties;
        this.processors = processors;
    }

    @Override
    protected void init() {
        eventTopicProperties.ensureTopicsExist(kafkaProperties);
        checkProcessorsEventTypes();

        Properties props = buildKafkaStreamsProps();
        Topology topology = getTopologyBuilder().build();
        initKafkaStreams(topology, props);
    }

    @Override
    protected void shutdown() {
        //'kafkaStreams is null' is possible in case of initialization failure in the init() method
        if (kafkaStreams != null && kafkaStreams.state().isRunningOrRebalancing()) {
            kafkaStreams.close();
        }
    }

    /**
     * Проверить, что для каждого типа событий процессоров определен топик
     */
    private void checkProcessorsEventTypes() {
        for (EventProcessor<? extends Event> processor : processors) {
            eventTopicProperties.resolveTopicByEventClass(processor.getEventToProcessType());

            if (processor instanceof TransformingEventProcessor) {
                TransformingEventProcessor<? extends Event, ? extends Event> transformingProcessor =
                        (TransformingEventProcessor<? extends Event, ? extends Event>) processor;
                eventTopicProperties.resolveTopicByEventClass(transformingProcessor.getResultEventType());
            }
        }
    }

    private TopologyBuilder getTopologyBuilder() {
        //todo: передатать получение сапплаеров без обращения к контексту,
        // т.к. при таком подходе процессоры создаются и до этого вызова
        // и потом при старте топологии еще N раз (N = кол-во тредов)

        return new TopologyBuilder(
                eventTopicProperties,
                processors,
                getTerminalEventProcessorSupplier(),
                getTransformingEventProcessorSupplier());
    }

    private TerminalEventProcessorSupplier getTerminalEventProcessorSupplier() {
        return processorClass -> (
                () -> (TerminalEventProcessor<? extends Event>) appContext.getBean(processorClass)
        );
    }

    private TransformingEventProcessorSupplier getTransformingEventProcessorSupplier() {
        return processorClass -> (
                () -> (TransformingEventProcessor<? extends Event, ? extends Event>) appContext.getBean(processorClass)
        );
    }

    /**
     * Сформировать параметры для KafkaStream
     */
    private Properties buildKafkaStreamsProps() {
        Properties props = new Properties();
        props.putAll(kafkaProperties.buildStreamsProperties());

        // при ошибке десериализации сообщения из топика - логируем и пропускаем это сообщение
        props.put(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        props.put(
                StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                runnerProperties.getStreamsThreadNumber());

        return props;
    }

    private void initKafkaStreams(Topology topology, Properties props) {
        logger.info("KafkaStreams initializing...");

        kafkaStreams = new KafkaStreams(topology, props);

        // обработчик исключений, которым будем ловить, например, падение кафка-стрима при удалении топика
        kafkaStreams.setUncaughtExceptionHandler((thread, throwable) -> {
            logger.error("KafkaStreams was failed unexpectedly at thread: " + thread.getName(), throwable);
            exit(1);
        });

        kafkaStreams.start();
        logger.info("KafkaStreams initialized.");
    }

}
