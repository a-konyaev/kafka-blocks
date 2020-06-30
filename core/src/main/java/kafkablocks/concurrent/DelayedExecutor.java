package kafkablocks.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

@Slf4j
public class DelayedExecutor<K, V> {
    private final Duration execTimeout;
    private final int itemsLimit;
    private final BiConsumer<K, V> action;

    private final Object lock = new Object();
    private final Map<K, V> map = new HashMap<>();
    private final WaitHandle itemsLimitExceeded = new WaitHandle();
    private final WaitHandle stopEvent = new WaitHandle();

    private final Thread thread;

    /**
     * @param execTimeout Timeout on reaching which execution starts
     * @param itemsLimit  Max items limit on reaching which execution starts
     * @param action      Action that executes items
     */
    public DelayedExecutor(@NotNull Duration execTimeout, @Positive int itemsLimit, @NotNull BiConsumer<K, V> action) {
        Assert.notNull(execTimeout, "execTimeout must not be null");
        Assert.isTrue(itemsLimit > 0, "itemsLimit must be greater than 0");
        Assert.notNull(action, "action must not be null");

        this.execTimeout = execTimeout;
        this.itemsLimit = itemsLimit;
        this.action = action;

        thread = new Thread(this::process, "delayed-executor");
        thread.start();
    }

    public void shutdown() {
        log.debug("Delayed executor stopping...");
        stopEvent.set();
    }

    public void addItem(K key, V value) {
        // todo synchronized - не лучший вариант, надо попробовать склабывать в concurrent queue, т.к.
        //  в ней эффективнее реализована блокировка, а уже из нее асинхронно забирать и обрабатывать
        synchronized (lock) {
            map.put(key, value);

            if (map.size() > itemsLimit) {
                itemsLimitExceeded.set();
            }
        }
    }

    public void addAllItems(Collection<Map.Entry<K, V>> items) {
        synchronized (lock) {
            for (Map.Entry<K, V> item : items) {
                map.put(item.getKey(), item.getValue());
            }

            if (map.size() > itemsLimit) {
                itemsLimitExceeded.set();
            }
        }
    }

    private void process() {
        while (true) {
            // ждем, но результат ожидания не важен - таймаут или лимит превышен, в любом случае начинаем обработку
            // за исключением, если выставлено событие остановки
            int occurredEventIndex = WaitHandle.waitAny(execTimeout.toMillis(), TimeUnit.MILLISECONDS, stopEvent, itemsLimitExceeded);
            if (occurredEventIndex == 0) {
                log.debug("Delayed executor thread interrupted");
                return;
            }

            ArrayList<Map.Entry<K, V>> items;
            synchronized (lock) {
                if (map.size() == 0) {
                    continue;
                }

                items = new ArrayList<>(map.entrySet());
                map.clear();
            }

            execute(items);
        }
    }

    private void execute(ArrayList<Map.Entry<K, V>> items) {
        for (Map.Entry<K, V> item : items) {
            try {
                action.accept(item.getKey(), item.getValue());
            } catch (RuntimeException e) {
                log.error("Execution failed for item: " + item, e);
            }
        }
    }
}