package kafkablocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


/**
 * Базовый класс для сервисов
 */
public abstract class ServiceBase {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    protected ApplicationContext appContext;
    /**
     * Признак того, что метод shutdown уже ранее был вызван
     */
    private boolean shutdown = false;


    @PostConstruct
    private void initInternal() {
        logger.info("Initializing...");

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownInternal));

        try {
            init();
            logger.info("Initialization completed.");
            return;

        } catch (Exception e) {
            logger.error("Initialization failed", e);
        }

        shutdownInternal();
        exit(1);
    }

    /**
     * Custom initialization in derived class
     */
    protected abstract void init();

    @PreDestroy
    private void shutdownInternal() {
        if (shutdown)
            return;

        logger.info("Shutdown...");

        shutdown();

        shutdown = true;
        logger.info("Shutdown completed.");
    }

    /**
     * Custom destroying in derived class
     */
    protected abstract void shutdown();

    protected void exit(int exitCode) {
        logger.info("Exiting with code " + exitCode);
        System.exit(SpringApplication.exit(appContext, () -> exitCode));
    }
}
