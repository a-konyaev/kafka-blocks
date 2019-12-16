package org.kafkablocks.utils;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class TimeUtilsTest {

    @Test
    public void toTimestamp() {
        LocalDateTime dateTime = LocalDateTime
                .of(2018, 12, 5, 20, 2, 33)
                .plusNanos(123000000); // plus 123 millis

        // do test only for Msc because test will fail at another region
        if (TimeUtils.DEFAULT_ZONE.getId().equals("Europe/Moscow"))
            Assert.assertEquals(1544029353123L, TimeUtils.toTimestamp(dateTime));
    }
}
