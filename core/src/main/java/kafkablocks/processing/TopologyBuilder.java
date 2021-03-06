package kafkablocks.processing;

import kafkablocks.EventTopicProperties;
import kafkablocks.serialization.SerdeProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import kafkablocks.events.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TopologyBuilder {

    private final EventTopicProperties eventTopicProperties;
    private final List<EventProcessor<? extends Event>> processors;
    private final TerminalEventProcessorSupplier terminalProcessorSupplier;
    private final TransformingEventProcessorSupplier transformingProcessorSupplier;

    /**
     * Создать топологию обработки
     */
    public Topology build() {
        log.debug("Topology building...");
        StreamsBuilder builder = new StreamsBuilder();

        TopologyNode rootNode = createTopologyTree();

        for (TopologyNode node : rootNode.children) {
            String topic = eventTopicProperties.resolveTopicByEventClass(node.getInputEventType());
            Consumed<String, ? extends Event> consumed = Consumed.with(Serdes.String(), SerdeProvider.getSerde(node.getInputEventType()));

            KStream<String, ? extends Event> stream = builder.stream(topic, consumed);
            node.setInputStream(stream);

            buildNode(builder, node);
        }

        Topology topology = builder.build();

        if (log.isDebugEnabled()) {
            log.debug(topology.describe().toString());
        }

        return topology;
    }

    @SuppressWarnings("unchecked")
    private void buildNode(StreamsBuilder builder, TopologyNode node) {
        EventProcessor<? extends Event> processor = node.getProcessor();
        String[] stateStoreNameArr = null;

        if (processor instanceof StateEventProcessor) {
            StateEventProcessor<? extends Event, ? extends EventProcessorState> stateProcessor =
                    (StateEventProcessor<? extends Event, ? extends EventProcessorState>) processor;

            builder.addStateStore(stateProcessor.getStateStoreBuilder());
            stateStoreNameArr = new String[1];
            stateStoreNameArr[0] = stateProcessor.getStateStoreName();
        }

        if (processor instanceof TransformingEventProcessor) {
            KStream<String, ? extends Event> outputStream = node.getInputStream()
                    .transform(
                            transformingProcessorSupplier.get(
                                    ((TransformingEventProcessor<? extends Event, ? extends Event>) processor)
                                            .getClass()),
                            stateStoreNameArr)
                    .through(
                            eventTopicProperties.resolveTopicByEventClass(node.getOutputEventType()),
                            Produced.with(Serdes.String(), SerdeProvider.getSerde(node.getOutputEventType())));

            node.setOutputStream(outputStream);

        } else if (processor instanceof TerminalEventProcessor) {
            node.getInputStream()
                    .process(
                            terminalProcessorSupplier.get(
                                    ((TerminalEventProcessor<? extends Event>) processor).getClass()),
                            stateStoreNameArr);

        } else if (processor instanceof StreamProcessor) {
            node.setOutputStream(((StreamProcessor) processor).process(node.getInputStream()));
        }

        for (TopologyNode child : node.children) {
            buildNode(builder, child);
        }
    }

    /**
     * Сформировать дерево топологии
     *
     * @return корень дерева топологии - это фиктивный пустой узел,
     * т.к. может быть несколько процессоров, которые претендуют на роль корневого узла
     */
    private TopologyNode createTopologyTree() {
        // корень дерева
        TopologyNode root = TopologyNode.EMPTY;
        // Таблица типов результирующих событий топологии
        // - ключ - тип события, которое является результатом обработки узла топологии (выходных событий)
        // - значение - узел топологии обработки
        Map<Class<? extends Event>, TopologyNode> resultEventMap = new HashMap<>();
        // список всех узлов
        List<TopologyNode> nodes = new ArrayList<>();

        // создаем узел для каждого процессора
        for (EventProcessor<? extends Event> processor : processors) {
            TopologyNode node;
            if (processor instanceof TransformingEventProcessor) {
                Class<? extends Event> resultEventType =
                        ((TransformingEventProcessor<? extends Event, ? extends Event>) processor).getResultEventType();

                node = TopologyNode.createNode(
                        processor.getEventToProcessType(),
                        resultEventType,
                        processor);

                TopologyNode existingNode = resultEventMap.get(resultEventType);
                if (existingNode != null)
                    throw new IllegalStateException(String.format(
                            "Cannot create node %s because topology already contains node with same result event type: %s",
                            node, existingNode));

                resultEventMap.put(resultEventType, node);

            } else {
                node = TopologyNode.createTerminalNode(processor.getEventToProcessType(), processor);
            }

            nodes.add(node);
        }

        // связываем узлы в дерево
        for (TopologyNode node : nodes) {
            TopologyNode parentNode = resultEventMap.get(node.getInputEventType());
            if (parentNode != null) {
                parentNode.addChild(node);
            } else {
                root.addChild(node);
            }
        }

        //todo: проверить, что нет циклов

        if (root.children.size() == 0) {
            log.warn(printAllNodes(nodes));
            throw new IllegalStateException("Topology creation error: source nodes undefined");
        }

        log.info(root.print());

        return root;
    }

    private String printAllNodes(List<TopologyNode> nodes) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("Topology nodes:");
        sb.append(System.lineSeparator());

        nodes.forEach(node -> {
            sb.append(node);
            sb.append(System.lineSeparator());
        });
        return sb.toString();
    }
}
