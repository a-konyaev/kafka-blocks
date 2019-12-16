package org.kafkablocks.processing;

import lombok.Getter;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.GenericTypeResolver;
import org.kafkablocks.events.Event;


/**
 * Базовый процессор
 * @param <EventToProcess>
 */
public abstract class BaseEventProcessor<EventToProcess extends Event>
        implements
        EventProcessor<EventToProcess> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Getter
    private final Class<EventToProcess> eventToProcessType;
    ProcessorContext context;

    @SuppressWarnings("unchecked")
    BaseEventProcessor() {
        eventToProcessType = (Class<EventToProcess>) GenericTypeResolver.resolveTypeArgument(
                getClass(), BaseEventProcessor.class);
    }

    public void init(ProcessorContext context) {
        this.context = context;
        initPunctuator();
        init();
    }

    protected abstract void init();

    /**
     * Инициализация пунктуатора
     */
    private void initPunctuator() {
        PunctuatorSupplier supplier = getPunctuatorSupplier();
        if (supplier == null)
            return;

        this.context.schedule(
                supplier.getIntervalMs(),
                supplier.getPunctuationType(),
                timestamp -> {
                    logger.debug("Punctuation starting...");
                    supplier.getPunctuator().punctuate(timestamp);
                    logger.debug("Punctuation done.");
                });
    }

    /**
     * Получить поставщика пунктуатора.
     * Метод должны переопределить классы-наследники, если им нужен пунктуатор
     *
     * @return поставщик пунктуатора или null, если пунктуатор не нужен
     */
    protected PunctuatorSupplier getPunctuatorSupplier() {
        return null;
    }
}
