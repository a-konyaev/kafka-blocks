package kafkablocks.utils;

import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafkablocks.concurrent.WaitHandle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class WaitHandleTest {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Тест проверяет, что ожидание WaitHandle-а (вызов метода wait(...))
     * корректно работает после его выставления (set) и сброса (reset)
     */
    @Test
    @SneakyThrows
    public void test_eventWaiting() {
        // не устанавливать большое число, т.к. запуск большого кол-ва тредов занимает много времени,
        // которое не учитывается в тесте
        final int CHECK_THREAD_COUNT = 3;
        CountDownLatch checkLatch = new CountDownLatch(CHECK_THREAD_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(CHECK_THREAD_COUNT);

        WaitHandle waitHandle = new WaitHandle();

        for (int i = 0; i < CHECK_THREAD_COUNT; i++) {
            runCheckThread(String.valueOf(i), waitHandle, 100, checkLatch, stopLatch);
        }

        CompletableFuture.runAsync(() -> {
            logger.debug("start thread X");

            ThreadUtils.safeDelay(150);
            waitHandle.set();
            waitHandle.reset();

            logger.debug("finish thread C");
        });

        if (!stopLatch.await(1, TimeUnit.SECONDS))
            Assert.fail("stopLatch timeout");

        long checkLatchCount = checkLatch.getCount();
        if (checkLatchCount != 0)
            Assert.fail("Check latch count is wrong: " + checkLatchCount);
    }

    private void runCheckThread(
            String threadName,
            WaitHandle waitHandle,
            long waitMillis,
            CountDownLatch checkLatch,
            CountDownLatch stopLatch) {

        CompletableFuture.runAsync(() -> {
            logger.debug("start thread {}", threadName);

            boolean expectFalse_1 = waitHandle.wait(waitMillis, TimeUnit.MILLISECONDS);

            // wait for another thread will set+reset waitHandle
            ThreadUtils.safeDelay(waitMillis);

            boolean expectFalse_2 = waitHandle.wait(waitMillis, TimeUnit.MILLISECONDS);

            if (!expectFalse_1 && !expectFalse_2)
                checkLatch.countDown();

            stopLatch.countDown();

            logger.debug("stop thread {}", threadName);
        });
    }

    @Test
    @SneakyThrows
    public void test_waitWithoutReset() {
        WaitHandle waitHandle = new WaitHandle();

        CompletableFuture.runAsync(() -> {
            ThreadUtils.safeDelay(50);
            waitHandle.set();
            ThreadUtils.safeDelay(50);
            waitHandle.reset();
        });

        // здесь мы должны ждать только на 1 итерации, а на остальных итерациях wait должен сразу же
        // возвращать true, что подтверждает то, что событие все еще "взведено" (не сброшено)
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue("waitHandle has not set in time", waitHandle.wait(60, TimeUnit.MILLISECONDS));
        }

        Assert.assertTrue("waitHandle must be set", waitHandle.isSet());

        ThreadUtils.safeDelay(100);
        // к этому моменту событие уже сброшено и wait должен возвращать false
        for (int i = 0; i < 3; i++) {
            Assert.assertFalse("waitHandle must be unset", waitHandle.wait(30, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    @SneakyThrows
    public void test_waitAny() {
        WaitHandle w0 = new WaitHandle();
        WaitHandle w1 = new WaitHandle();
        WaitHandle[] handles = {w0, w1};

        CompletableFuture.runAsync(() -> {
            ThreadUtils.safeDelay(30);
            w0.set();
            ThreadUtils.safeDelay(30);
            w1.set();
        });

        reset(handles);
        Assert.assertEquals("case 1", 0, waitAny(50, handles));

        reset(handles);
        Assert.assertEquals("case 2", 1, waitAny(50, handles));

        // т.к. w1 не сбросили, то должны сразу же получить его индекс
        Assert.assertFalse(w0.isSet());
        Assert.assertTrue(w1.isSet());
        Assert.assertEquals("case 3", 1, waitAny(5000, handles));

        reset(handles);
        Assert.assertEquals("case 4", WaitHandle.WAIT_TIMEOUT, waitAny(30, handles));
    }

    @Test
    @SneakyThrows
    public void test_waitAnyWhileSetReset() {
        WaitHandle w0 = new WaitHandle();
        WaitHandle w1 = new WaitHandle();
        WaitHandle w2 = new WaitHandle();
        WaitHandle[] handles = {w0, w1, w2};

        CompletableFuture.runAsync(() -> {
            ThreadUtils.safeDelay(100);
            w1.set();
            w1.reset();
        });

        Assert.assertEquals("waitAny returns index != 1", 1, waitAny(300, handles));
    }

    private void reset(WaitHandle... waitHandles) {
        for (WaitHandle waitHandle : waitHandles)
            waitHandle.reset();
    }

    private int waitAny(long timeoutMillis, WaitHandle... waitHandles) {
        return WaitHandle.waitAny(timeoutMillis, TimeUnit.MILLISECONDS, waitHandles);
    }
}
