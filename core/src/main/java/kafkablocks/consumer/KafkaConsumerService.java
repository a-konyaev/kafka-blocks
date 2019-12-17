package kafkablocks.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import kafkablocks.ServiceBase;
import kafkablocks.events.Event;
import kafkablocks.EventTopicProperties;


/**
 * Сервис "Потребителя".
 * Используется для автоматического создания экземпляра потребителя средствами Spring-а.
 * Внутри использует KafkaMultipleConsumer реализацию потребителя.
 */
@Service
@EnableConfigurationProperties({KafkaConsumerProperties.class, EventTopicProperties.class, KafkaProperties.class})
public class KafkaConsumerService extends ServiceBase implements KafkaConsumer {
    private final KafkaConsumerProperties consumerProperties;
    private ConsumingParams consumingParams;
    private final EventTopicProperties eventTopicProperties;
    private final KafkaProperties kafkaProperties;
    private final KafkaConsumer internalConsumer;


    @Autowired
    public KafkaConsumerService(
            KafkaConsumerProperties consumerProperties,
            EventTopicProperties eventTopicProperties,
            KafkaProperties kafkaProperties) {

        this.consumerProperties = consumerProperties;
        this.eventTopicProperties = eventTopicProperties;
        this.kafkaProperties = kafkaProperties;
        internalConsumer = new KafkaMultipleConsumer(eventTopicProperties, kafkaProperties);
    }

    @Override
    protected void init() {
        eventTopicProperties.ensureTopicsExist(kafkaProperties);
    }

    @Override
    protected void shutdown() {
        if (internalConsumer.isRunning())
            internalConsumer.stop();
    }

    @Override
    public String getId() {
        return internalConsumer.getId();
    }

    @Override
    public void setIdleEventInterval(int sec) {
        internalConsumer.setIdleEventInterval(sec);
    }

    @Override
    public void setPhaseChangedHandler(PhaseChangedHandler handler) {
        internalConsumer.setPhaseChangedHandler(handler);
    }

    @Override
    public void setErrorHandler(ErrorHandler errorHandler) {
        internalConsumer.setErrorHandler(errorHandler);
    }

    @Override
    public void setIdleHandler(Runnable idleHandler) {
        internalConsumer.setIdleHandler(idleHandler);
    }

    @Override
    public void setPlaybackTimeHandler(PlaybackTimeHandler playbackTimeHandler) {
        internalConsumer.setPlaybackTimeHandler(playbackTimeHandler);
    }

    @Override
    public <T extends Event> void setEventHandler(EventHandler<T> eventHandler, Class<T> eventType) {
        internalConsumer.setEventHandler(eventHandler, eventType);
    }

    @Override
    public void setConsumingParams(ConsumingParams params) {
        consumingParams = params;
    }

    @Override
    public void changeRate(double rate) {
        internalConsumer.changeRate(rate);
    }

    @Override
    public void start() {
        if (consumingParams != null) {
            internalConsumer.setConsumingParams(consumingParams);
        }
        else {
            ConsumingParams params = consumerProperties.getConsumingParams();
            internalConsumer.setConsumingParams(params);
        }

        internalConsumer.start();
    }

    @Override
    public void stop() {
        internalConsumer.stop();
    }

    @Override
    public boolean isRunning() {
        return internalConsumer.isRunning();
    }

    @Override
    public void pause() {
        internalConsumer.pause();
    }

    @Override
    public void resume() {
        internalConsumer.resume();
    }

    @Override
    public KafkaConsumerPhase getPhase() {
        return internalConsumer.getPhase();
    }

    @Override
    public void addFilter(Filter filter) {
        internalConsumer.addFilter(filter);
    }

    @Override
    public void resetFilters() {
        internalConsumer.resetFilters();
    }
}
