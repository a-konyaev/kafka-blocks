package org.kafkablocks.utils;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;


public class ActionUtilsTest {
    final static int MAX_ATTEMPTS = 3;

    @Test
    public void doSeveralAttempts_FirstOK() {

        AtomicInteger attemptCounter = new AtomicInteger(0);

        Boolean res = ActionUtils.doSeveralAttempts(() -> {
                    attemptCounter.incrementAndGet();
                    return true;
                },
                MAX_ATTEMPTS, Duration.ofMillis(50));

        Assert.assertTrue(res);
        Assert.assertEquals(1, attemptCounter.get());
    }

    @Test
    public void doSeveralAttempts_LastOK() {
        AtomicInteger attemptCounter = new AtomicInteger(0);

        Boolean res = ActionUtils.doSeveralAttempts(
                () -> {
                    int a = attemptCounter.incrementAndGet();
                    if (a == MAX_ATTEMPTS)
                        return true;

                    throw new RuntimeException("attempt = " + a);

                }, MAX_ATTEMPTS, Duration.ofMillis(50));

        Assert.assertTrue(res);
        Assert.assertEquals(MAX_ATTEMPTS, attemptCounter.get());
    }

    @Test
    public void doSeveralAttempts_AllFailed() {
        AtomicInteger attemptCounter = new AtomicInteger(0);

        try {
            ActionUtils.doSeveralAttempts(
                    () -> {
                        int a = attemptCounter.incrementAndGet();
                        throw new RuntimeException("attempt = " + a);

                    }, MAX_ATTEMPTS, Duration.ofMillis(50));

            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(MAX_ATTEMPTS, attemptCounter.get());
            Assert.assertEquals("attempt = " + MAX_ATTEMPTS, e.getMessage());
        }
    }

    //@Test
    public void doSeveralAttempts_StopEventRaised() {
        //todo
    }
}
