package org.kafkablocks.examples;

import lombok.RequiredArgsConstructor;
import org.kafkablocks.examples.events.AEvent;
import org.kafkablocks.examples.events.BEvent;
import org.kafkablocks.processing.BaseTransformingEventProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Scope("prototype")
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class TestProcessor
        extends BaseTransformingEventProcessor<AEvent, BEvent> {

    @Override
    protected List<BEvent> process(String key, AEvent aEvent) {
        //todo
        return null;
    }
}
