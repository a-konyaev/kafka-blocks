package kafkablocks.examples.core.processing;

import kafkablocks.examples.events.AccelerationEvent;
import kafkablocks.examples.events.DistanceEvent;
import kafkablocks.processing.BaseTransformingEventProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Scope("prototype")
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class AccelerationProcessor extends BaseTransformingEventProcessor<DistanceEvent, AccelerationEvent> {

    @Override
    protected List<AccelerationEvent> processEvent(String key, DistanceEvent distanceEvent) {
        return null;
    }
}
