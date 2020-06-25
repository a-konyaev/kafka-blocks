package kafkablocks.examples.prioritizer.consumer2;

import kafkablocks.examples.prioritizer.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class BusinessService {

    public void processHigh(String value) {
        log.info("process " + value);
        Constants.sleepAbout(Constants.MESSAGE_PROCESSING_TIME);
    }

    public void processLow(String value) {
        log.info("process " + value);
        Constants.sleepAbout(Constants.MESSAGE_PROCESSING_TIME);
    }
}
