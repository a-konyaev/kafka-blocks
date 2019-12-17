package kafkablocks.utils;

import lombok.SneakyThrows;
import kafkablocks.concurrent.WaitHandle;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public final class ActionUtils {
    private ActionUtils() {
    }


    public static boolean doSeveralAttempts(
            Runnable runnable,
            int maxAttempts,
            Duration delayBetweenAttempts) {

        return doSeveralAttempts(runnable, maxAttempts, delayBetweenAttempts, null);
    }

    public static boolean doSeveralAttempts(
            Runnable runnable,
            int maxAttempts,
            Duration delayBetweenAttempts,
            WaitHandle stopEvent) {

        Boolean res = doSeveralAttempts(() -> {
                    runnable.run();
                    return true;
                },
                maxAttempts,
                delayBetweenAttempts,
                stopEvent);

        return res == null ? false : res;
    }

    public static <V> V doSeveralAttempts(
            Callable<V> callable,
            int maxAttempts,
            Duration delayBetweenAttempts) {

        return doSeveralAttempts(callable, maxAttempts, delayBetweenAttempts, null);
    }

    @SneakyThrows
    public static <V> V doSeveralAttempts(
            Callable<V> action,
            int maxAttempts,
            Duration delayBetweenAttempts,
            WaitHandle stopEvent) {

        int counter = 0;
        while (true) {
            try {
                return action.call();

            } catch (Throwable throwable) {
                if (++counter == maxAttempts)
                    throw throwable;

                long delayMillis = delayBetweenAttempts.toMillis();

                if (stopEvent == null) {
                    Thread.sleep(delayMillis);
                } else if (stopEvent.wait(delayMillis, TimeUnit.MILLISECONDS)) {
                    return null;
                }
            }
        }
    }
}
