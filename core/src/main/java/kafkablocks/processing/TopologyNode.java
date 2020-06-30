package kafkablocks.processing;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.streams.kstream.KStream;
import kafkablocks.events.Event;

import java.util.LinkedList;
import java.util.List;

/**
 * Узел топологии обработки.
 * Имеет ссылку на родительский узел, список дочерних узлов и данные узла,
 * которые определяются:
 * - типом входных событий
 * - процессором
 * - типом выходных событий, т.е. событий, которые порождает процессор узла.
 * Если процессор терминальный, то тип выходных событий не определен.
 */
class TopologyNode {
    static final TopologyNode EMPTY = new TopologyNode(null, null, null);

    TopologyNode parent;
    final List<TopologyNode> children = new LinkedList<>();

    @Getter
    private final Class<? extends Event> inputEventType;
    @Getter
    private final Class<? extends Event> outputEventType;
    @Getter
    private final EventProcessor<? extends Event> processor;

    @Setter
    private KStream<String, ? extends Event> inputStream;
    @Setter
    private KStream<String, ? extends Event> outputStream;


    KStream<String, ? extends Event> getInputStream() {
        if (inputStream != null)
            return inputStream;

        if (parent == null || parent.outputStream == null)
            throw new RuntimeException("Input stream undefined");

        return parent.outputStream;
    }


    private TopologyNode(
            Class<? extends Event> inputEventType,
            Class<? extends Event> outputEventType,
            EventProcessor<? extends Event> processor) {

        this.inputEventType = inputEventType;
        this.outputEventType = outputEventType;
        this.processor = processor;
    }

    static TopologyNode createNode(
            Class<? extends Event> inputEventType,
            Class<? extends Event> outputEventType,
            EventProcessor<? extends Event> processor) {
        return new TopologyNode(inputEventType, outputEventType, processor);
    }

    static TopologyNode createTerminalNode(
            Class<? extends Event> inputEventType,
            EventProcessor<? extends Event> processor) {
        return new TopologyNode(inputEventType, null, processor);
    }

    void addChild(TopologyNode child) {
        child.parent = this;
        this.children.add(child);
    }

    String print() {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("Topology tree:");
        sb.append(System.lineSeparator());
        print(sb, 0);
        return sb.toString();
    }

    private void print(StringBuilder sb, int level) {
        for (int i = 0; i < level; i++) {
            sb.append(' ');
            sb.append(' ');
            sb.append("  ");
        }
        sb.append(this.toString());
        sb.append(System.lineSeparator());

        for (TopologyNode child : children) {
            child.print(sb, level + 1);
        }
    }

    @Override
    public String toString() {
        if (this == EMPTY)
            return "";

        String outputLabel = getOutputLabel(processor, inputEventType, outputEventType);

        return String.format("%s -> [%s] -> %s",
                inputEventType.getSimpleName(),
                processor.getClass().getSimpleName(),
                outputLabel);
    }

    private static String getOutputLabel(
            EventProcessor<? extends Event> processor,
            Class<? extends Event> inputEventType,
            Class<? extends Event> outputEventType) {

        if (processor instanceof StreamProcessor) {
            return inputEventType.getSimpleName();
        }

        return outputEventType == null
                ? "x"
                : outputEventType.getSimpleName();
    }
}
