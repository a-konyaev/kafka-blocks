package org.kafkablocks.processing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.kafkablocks.EventTopicProperties;
import org.kafkablocks.serialization.SerdeProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TopologyBuilder {

    private final EventTopicProperties eventTopicProperties;
    private final List<EventProcessor> processors;
    private final TerminalEventProcessorSupplier terminalProcessorSupplier;
    private final TransformingEventProcessorSupplier transformingProcessorSupplier;

    /**
     * Создать топологию обработки
     */
    @SuppressWarnings("unchecked")
    public Topology build() {
        log.debug("Topology building...");
        StreamsBuilder builder = new StreamsBuilder();

        TopologyNode rootNode = createTopologyTree();

        for (TopologyNode node : rootNode.children) {
            KStream stream = builder.stream(
                    eventTopicProperties.resolveTopicByEventClass(node.getInputEventType()),
                    Consumed.with(Serdes.String(), SerdeProvider.getSerde(node.getInputEventType())));
            node.setInputStream(stream);

            buildNode(builder, node);
        }

        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private void buildNode(StreamsBuilder builder, TopologyNode node) {
        EventProcessor processor = node.getProcessor();
        String[] stateStoreNameArr = null;
        if (processor instanceof StateEventProcessor) {
            StateEventProcessor stateProcessor = (StateEventProcessor) processor;
            builder.addStateStore(stateProcessor.getStateStoreBuilder());
            stateStoreNameArr = new String[1];
            stateStoreNameArr[0] = stateProcessor.getStateStoreName();
        }

        if (processor instanceof TransformingEventProcessor) {
            KStream stream = node.getInputStream()
                    .transform(
                            transformingProcessorSupplier.get(((TransformingEventProcessor) processor).getClass()),
                            stateStoreNameArr)
                    .through(
                            eventTopicProperties.resolveTopicByEventClass(node.getOutputEventType()),
                            Produced.with(Serdes.String(), SerdeProvider.getSerde(node.getOutputEventType())));

            node.setOutputStream(stream);

        } else if (processor instanceof TerminalEventProcessor) {
            node.getInputStream()
                    .process(
                            terminalProcessorSupplier.get(((TerminalEventProcessor) processor).getClass()),
                            stateStoreNameArr);
        }

        for (TopologyNode child : node.children) {
            buildNode(builder, child);
        }
    }

    /**
     * Сформировать дерево топологии
     *
     * @return корень дерева топологии - это фиктивный пустой узел,
     * т.к. может быть несколько процессоров, которые претендуют на роль узла
     */
    private TopologyNode createTopologyTree() {
        // корень дерева
        TopologyNode root = TopologyNode.EMPTY;
        // Таблица типов результирующих событий топологии
        // - ключ - тип события, которое является результатом обработки узла топологии (выходных событий)
        // - значение - узел топологии обработки
        Map<Class, TopologyNode> resultEventMap = new HashMap<>();
        // список всех узлов
        List<TopologyNode> nodes = new ArrayList<>();

        // создаем узел для каждого процессора
        for (EventProcessor processor : processors) {
            TopologyNode node;
            if (processor instanceof TransformingEventProcessor) {
                Class resultEventType = ((TransformingEventProcessor) processor).getResultEventType();
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
