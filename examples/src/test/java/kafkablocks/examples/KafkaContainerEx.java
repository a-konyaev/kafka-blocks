package kafkablocks.examples;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.util.CollectionUtils;
import org.testcontainers.containers.KafkaContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaContainerEx extends KafkaContainer {

    private final List<NewTopic> topics;

    public KafkaContainerEx(List<NewTopic> topics) {
        this.topics = topics;
    }

    @Override
    protected void doStart() {
        super.doStart();
        waitUntilContainerStarted();
        createTopics();
    }

    private void createTopics() {
        if (CollectionUtils.isEmpty(topics)) {
            return;
        }

        try (AdminClient adminClient = createAdminClient()) {
            adminClient.createTopics(topics);
        }
    }

    public Map<String, Object> getAdminClientConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, super.getBootstrapServers());
        return config;
    }

    public AdminClient createAdminClient() {
        return AdminClient.create(getAdminClientConfig());
    }
}
