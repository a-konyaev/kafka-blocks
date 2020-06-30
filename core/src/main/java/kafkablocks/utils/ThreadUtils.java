package kafkablocks.utils;

import static java.lang.Thread.currentThread;

public final class ThreadUtils {
    private ThreadUtils() {
    }

    /**
     * Вызывает Thread.sleep с заданным кол-вом секунд.
     *
     * @param sec кол-во секунд
     * @return true - ожидание завершено, false - ожидание было предвано
     */
    public static boolean safeDelaySec(long sec) {
        return safeDelay(sec * 1000);
    }

    /**
     * Вызывает Thread.sleep с заданным кол-вом миллисекунд.
     *
     * @param millis кол-во миллисекунд
     * @return true - ожидание завершено, false - ожидание было предвано
     */
    public static boolean safeDelay(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    public static Thread startNewThread(Runnable target, String name) {
        Thread thread = new Thread(target, name);
        thread.start();
        return thread;
    }

    public static void joinThread(Thread thread) {
        joinThread(thread, 100);
    }

    public static void joinThread(Thread thread, long millis) {
        if (thread == null)
            return;

        try {
            thread.join(millis);
            thread.interrupt();
        } catch (SecurityException | InterruptedException ignore) {
        }
    }
}
