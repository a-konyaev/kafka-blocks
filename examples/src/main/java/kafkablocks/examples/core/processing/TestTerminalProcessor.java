//package kafkablocks.examples.core.processing;
//
//import kafkablocks.examples.events.PositionEvent;
//import kafkablocks.processing.BaseTerminalEventProcessor;
//import lombok.RequiredArgsConstructor;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Scope;
//import org.springframework.stereotype.Service;
//
//@Service
//@Scope("prototype")
//@RequiredArgsConstructor(onConstructor_ = {@Autowired})
//public class TestTerminalProcessor
//        extends BaseTerminalEventProcessor<PositionEvent> {
//
//    @Override
//    protected void processEvent(String key, PositionEvent positionEvent) {
//        //todo
//        // https://github.com/a-konyaev/kafka-blocks/issues/1
//    }
//}
