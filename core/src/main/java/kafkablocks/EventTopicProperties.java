package kafkablocks;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;
import org.springframework.validation.annotation.Validated;
import kafkablocks.events.Event;
import kafkablocks.utils.ClassUtils;
import kafkablocks.utils.KafkaUtils;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@EqualsAndHashCode(callSuper = true)
@Validated
@ConfigurationProperties(prefix = "kafkablocks")
public class EventTopicProperties extends AppProperties {
    /**
     * Имя св-ва, в которое будут записаны имена топиков через запятую
     */
    public static final String TOPICS_PROPERTY_KEY = "kafkablocks.topics";

    /**
     * Имя пакета, в котором будем искать класс, если его имя является полным,
     * т.е. указано без имени пакета
     */
    // todo: сделать возможность указывать несколько пакетов
    @Getter
    @Setter
    private String packageName = "kafkablocks.events.impl";

    /**
     * Таблица: ключ - имя класса события, значение - имя топика
     */
    @Getter
    private final Map<String, String> eventTopic = new HashMap<>();
    /**
     * Таймаут, в течение которого ждем появления нужных топиков (сек).
     * Дотустимые значения от 1 до 600 (10 мин).
     * По умолчанию 3 мин (180 сек)
     */
    @Min(1)
    @Max(600)
    @Getter
    @Setter
    private int waitTopicsExistTimeout = 180;

    // region трюк с singleton-ом бина

    // singleton нужен для того, чтобы до него можно было добраться из классов,
    // которые инициализируются вручную (не Spring-ом), в частности, из EventDeserializer-а
    // todo: может заменить трюк на использование интерфейса SmartInitializingSingleton?

    private static Object instance;

    public static EventTopicProperties getInstance() {
        Assert.notNull(instance, "EventTopicProperties not initialized");
        return (EventTopicProperties) instance;
    }

    public EventTopicProperties() {
        instance = this;
    }

    //endregion

    //region initialization

    /**
     * Прямая таблица: имя топика -> класс события
     */
    private final Map<String, Class<? extends Event>> topic2EventClassMap = new HashMap<>();
    /**
     * Обратная таблица: класс события -> имя топика
     */
    private final Map<Class<? extends Event>, String> eventClass2TopicMap = new HashMap<>();

    @Override
    protected void init() {
        Assert.notEmpty(eventTopic, "eventTopic must not be empty");

        // заполняем две таблицы - прямую и обратную, при этом проверям,
        // чтобы соответствие топик <-> класс события были один к одному
        eventTopic.forEach((eventClassName, topic) -> {
            // проверим, что для данного топика еще не определен класс события
            if (topic2EventClassMap.containsKey(topic))
                // todo: сделать возможность для одного топика задавать несколько классов и наоборот,
                //  для одного класса задавать несколько топиков
                //  ! для ключа хранить список, а если будет необходимость выбрать по ключу одно значение,
                //  то брать первый эл-т списка
                throw new RuntimeException("More than one event class has been defined for a topic: " + topic);

            Class<? extends Event> clazz = loadEventClass(eventClassName);

            // проверим, что для данного класса события еще не определен топик
            if (eventClass2TopicMap.containsKey(clazz))
                throw new RuntimeException("More than one topic has been defined for a event class: " + clazz.getName());

            topic2EventClassMap.put(topic, clazz);
            eventClass2TopicMap.put(clazz, topic);
        });

        // перечень всех топиков записываем в св-во
        String topics = String.join(",", getAllTopics());
        System.setProperty(TOPICS_PROPERTY_KEY, topics);
    }

    private Class<? extends Event> loadEventClass(String eventClassName) {
        // если имя класса содержит точку, то считаем, что задано полное имя класса
        if (eventClassName.contains("."))
            return ClassUtils.loadClass(eventClassName, Event.class);

        // иначе, грузим класс из пакета по умолчанию
        return ClassUtils.loadClass(packageName, eventClassName, Event.class);
    }

    //endregion

    public Set<String> getAllTopics() {
        return topic2EventClassMap.keySet();
    }

    public Set<Class<? extends Event>> getAllEventClasses() {
        return eventClass2TopicMap.keySet();
    }

    public String resolveTopicByEvent(Event event) {
        return resolveTopicByEventClass(event.getClass());
    }

    /**
     * Находит топик по классу события. Если для заданного класса события топик не найден,
     * то рекурсивно выполняет поиск для базового класса.
     */
    public String resolveTopicByEventClass(Class<? extends Event> eventClass) {
        String topic = ClassUtils.getValueFromClassKeyMap(eventClass, eventClass2TopicMap);
        if (topic == null)
            throw new RuntimeException("Topic not defined for event class: " + eventClass.getName());

        return topic;
    }

    public Class<? extends Event> resolveEventClassByTopic(String topic) {
        Class<? extends Event> clazz = topic2EventClassMap.get(topic);
        if (clazz == null)
            throw new RuntimeException("Event class not defined for topic: " + topic);

        return clazz;
    }

    public void ensureTopicsExist(KafkaProperties kafkaProperties) {
        KafkaUtils.ensureTopicsExist(kafkaProperties, getAllTopics(), waitTopicsExistTimeout);
    }

    public void ensureTopicsExist(Properties props) {
        KafkaUtils.ensureTopicsExist(props, getAllTopics(), waitTopicsExistTimeout);
    }
}
