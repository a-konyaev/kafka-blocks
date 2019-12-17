package kafkablocks.examples;

import kafkablocks.examples.events.DistanceEvent;
import kafkablocks.examples.events.SpeedEvent;
import kafkablocks.processing.BaseTransformingEventProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Scope("prototype")
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class SpeedProcessor
        extends BaseTransformingEventProcessor<DistanceEvent, SpeedEvent> {

    @Override
    protected List<SpeedEvent> process(String key, DistanceEvent distanceEvent) {
        return null;
    }
}
