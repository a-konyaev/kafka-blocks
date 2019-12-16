package org.kafkablocks.concurrent;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Примитив для синхронного и асинхронного ожидания события.
 * Требует ручного сброса после выставления.
 * Создавался как аналог ManualResetEvent из .Net
 * <p>
 * NOTE:
 * если поток А ждет хендл, а в потоке Б будет вызваны друг за другом set(); reset();
 * то поток А проснется, но метод wait() вернет false, т.к. isSet успеет получит значение false при вызове reset()
 */
public final class WaitHandle {
    public static final int WAIT_TIMEOUT = -1;

    private final Object monitor = new Object();
    private volatile boolean isSet = false;
    private volatile WaitAnyLatch latch;


    public void set() {
        synchronized (monitor) {
            isSet = true;
            monitor.notifyAll();

            if (latch != null) {
                latch.signal(this);
            }
        }
    }

    public void reset() {
        isSet = false;
    }

    public boolean isSet() {
        return isSet;
    }

    private void assignLatch(WaitAnyLatch latch) {
        synchronized (monitor) {
            this.latch = latch;

            if (isSet) {
                this.latch.signal(this);
            }
        }
    }

    /**
     * Waits for the event until it happens or the waiting time expires
     *
     * @param timeout waiting time
     * @return true - if the event has occurred,
     * false - if the timeout has expired or an exception has been thrown
     */
    public boolean wait(Duration timeout) {
        return wait(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Waits for the event until it happens or the waiting time expires
     *
     * @param timeout waiting time
     * @param unit    waiting time unit
     * @return true - if the event has occurred,
     * false - if the timeout has expired or an exception has been thrown
     */
    public boolean wait(long timeout, TimeUnit unit) {
        assertTimeout(timeout, unit);

        synchronized (monitor) {
            if (isSet)
                return true;

            try {
                monitor.wait(timeout == 0 ? 0 : unit.toMillis(timeout));
                return isSet;
            } catch (InterruptedException e) {
                return false;
            }
        }
    }

    public CompletableFuture<Boolean> waitAsync(long timeout, TimeUnit unit) {
        return CompletableFuture.supplyAsync(() -> wait(timeout, unit));
    }

    public static int waitAny(WaitHandle... waitHandles) {
        return waitAny(0, null, waitHandles);
    }

    /**
     * Wait for any of the handles during the {code timeout}
     * NOTE: this methods shouldn't be called from different threads for the same WaitHandle
     *
     * @param waitHandles array of the handles to wait
     * @param timeout     max time to waiting; 0 - wait forever
     * @param unit        unit of {code timeout}
     * @return if one/several of the {code waitHandles} occurred,
     * then this is the index of the (first) object in the array,
     * otherwise WAIT_TIMEOUT
     */
    public static int waitAny(long timeout, TimeUnit unit, WaitHandle... waitHandles) {
        assertTimeout(timeout, unit);

        if (waitHandles == null || waitHandles.length == 0)
            throw new IllegalArgumentException("waitHandles");

        WaitAnyLatch latch = new WaitAnyLatch(waitHandles);

        try {
            return timeout == 0
                    ? latch.await()
                    : latch.await(timeout, unit);
        } catch (InterruptedException e) {
            return WAIT_TIMEOUT;
        }
    }

    private static void assertTimeout(long timeout, TimeUnit unit) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout must be 0 (wait forever) or more");

        if (timeout > 0 && unit == null)
            throw new IllegalArgumentException("Unit for non zero timeout must be set");
    }

    private static class WaitAnyLatch {
        final CountDownLatch latch;
        final WaitHandle[] waitHandles;
        volatile int signaledIndex = WAIT_TIMEOUT;

        WaitAnyLatch(WaitHandle[] waitHandles) {
            this.latch = new CountDownLatch(1);
            this.waitHandles = waitHandles;

            for (WaitHandle waitHandle : waitHandles) {
                waitHandle.assignLatch(this);
            }
        }

        void signal(WaitHandle signaledWaitHandle) {
            // т.к. массив waitHandle-ов обычно маленького размера, то просто сделаем перебор
            for (int i = 0; i < waitHandles.length; i++) {
                if (waitHandles[i] == signaledWaitHandle) {
                    signaledIndex = i;
                    break;
                }
            }

            latch.countDown();
        }

        int await() throws InterruptedException {
            latch.await();
            return signaledIndex;
        }

        int await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit)
                    ? signaledIndex
                    : WAIT_TIMEOUT;
        }
    }
}
