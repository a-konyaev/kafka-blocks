package kafkablocks.examples;

import lombok.RequiredArgsConstructor;
import kafkablocks.examples.events.PositionEvent;
import kafkablocks.examples.events.DistanceEvent;
import kafkablocks.processing.BaseTransformingEventProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Scope("prototype")
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class DistanceProcessor
        extends BaseTransformingEventProcessor<PositionEvent, DistanceEvent> {

    @Override
    protected List<DistanceEvent> process(String key, PositionEvent aEvent) {
        //todo
        return null;
    }
}
