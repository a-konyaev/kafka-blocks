package kafkablocks.processing;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.streams.kstream.KStream;

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
    private final Class inputEventType;
    @Getter
    private final Class outputEventType;
    @Getter
    private final EventProcessor processor;

    @Setter
    private KStream inputStream;
    @Setter
    private KStream outputStream;

    KStream getInputStream() {
        if (inputStream != null)
            return inputStream;

        if (parent == null || parent.outputStream == null)
            throw new RuntimeException("Input stream undefined");

        return parent.outputStream;
    }


    private TopologyNode(Class inputEventType, Class outputEventType, EventProcessor processor) {
        this.inputEventType = inputEventType;
        this.outputEventType = outputEventType;
        this.processor = processor;
    }

    static TopologyNode createNode(Class inputEventType, Class outputEventType, EventProcessor processor) {
        return new TopologyNode(inputEventType, outputEventType, processor);
    }

    static TopologyNode createTerminalNode(Class inputEventType, EventProcessor processor) {
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

        return String.format("%s -> [%s] -> %s",
                inputEventType.getSimpleName(),
                processor.getClass().getSimpleName(),
                outputEventType == null ? "x" : outputEventType.getSimpleName());
    }
}
