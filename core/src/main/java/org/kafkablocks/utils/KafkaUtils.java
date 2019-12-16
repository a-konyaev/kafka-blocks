package org.kafkablocks.utils;

import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Утилиты для работы с Кафкой
 */
public final class KafkaUtils {
    private KafkaUtils() {
    }

    /**
     * Логгер
     */
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * Получить набор имен топиков, которые существуют в данный момент
     */
    private static Set<String> getExistingTopicNames(AdminClient adminClient) {
        try {
            return adminClient.listTopics().names().get();
        } catch (Exception e) {
            throw new RuntimeException("Error while getting topic names", e);
        }
    }

    /**
     * Проверить, существуют ли заданные топики в данный момент
     *
     * @return
     * null - все топики существуют,
     * не пустой список - список топиков, которых нет
     */
    private static List<String> checkIfTopicsExist(AdminClient adminClient, Set<String> requiredTopicNames) {
        logger.debug("Checking if topics {} exist...", requiredTopicNames);

        if (CollectionUtils.isEmpty(requiredTopicNames))
            throw new IllegalArgumentException("requiredTopicNames must be not empty");

        Set<String> existingTopicNames = getExistingTopicNames(adminClient);
        logger.debug("Currently existing topics: {}", existingTopicNames);

        boolean allExist = existingTopicNames.containsAll(requiredTopicNames);
        if (allExist) {
            logger.debug("All required topics exist");
            return null;
        }

        List<String> notExistingTopicNames = requiredTopicNames.stream()
                .filter(name -> !existingTopicNames.contains(name))
                .collect(Collectors.toList());

        logger.debug("Topics not found: {}", notExistingTopicNames);
        return notExistingTopicNames;
    }

    /**
     * Проверить, существуют ли заданные топики в данный момент.
     * Если заданных топиков нет или есть, но не все, то метод будет ждать их создания,
     * но не дольше, чем timeoutSeconds
     *
     * @param requiredTopicNames набор топиков
     * @param timeoutSeconds     таймаут ожидания создания топиков в секундах
     */
    @SneakyThrows
    private static boolean checkIfTopicsExist(
            AdminClient adminClient,
            Set<String> requiredTopicNames,
            int timeoutSeconds) {

        logger.info("Checking if topics {} exist (timeout = {} sec)...", requiredTopicNames, timeoutSeconds);

        while (true) {
            List<String> notExistingTopics = checkIfTopicsExist(adminClient, requiredTopicNames);
            if (notExistingTopics == null)
                return true;

            if (--timeoutSeconds <= 0) {
                logger.error("Waiting timeout for topics has expired. Not found topics: " + notExistingTopics);
                return false;
            }

            logger.debug("Topics not exist. Waiting 1 sec and try again...");
            TimeUnit.SECONDS.sleep(1);
        }
    }

    public static void ensureTopicsExist(
            KafkaProperties kafkaProperties, Set<String> requiredTopicNames, int timeoutSeconds) {
        ensureTopicsExist(kafkaProperties.buildConsumerProperties(), requiredTopicNames, timeoutSeconds);
    }

    @SuppressWarnings("unchecked")
    public static void ensureTopicsExist(
            Properties properties, Set<String> requiredTopicNames, int timeoutSeconds) {
        ensureTopicsExist((Map) properties, requiredTopicNames, timeoutSeconds);
    }

    /**
     * Убедиться, что топики существуют.
     * Если в течение заданного таймаута не удалось определить наличие всех топиков, то генерируется исключение
     */
    private static void ensureTopicsExist(
            Map<String, Object> adminKafkaProps, Set<String> requiredTopicNames, int timeoutSeconds) {

        try (AdminClient adminClient = AdminClient.create(adminKafkaProps)) {
            boolean allExist = KafkaUtils.checkIfTopicsExist(adminClient, requiredTopicNames, timeoutSeconds);
            if (!allExist)
                throw new RuntimeException("Not all required topics exist!");
        }
    }
}
