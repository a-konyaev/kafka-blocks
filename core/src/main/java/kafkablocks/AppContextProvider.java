package kafkablocks;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

/**
 * Этот компонент удобно использовать в методах, которые выполняются в треде вне Спринга,
 * но где все таки нужно добраться до контекста.
 * <p>
 * НО чтобы этот компонент был создан спрингом, нужно импортировать его: @Import(AppContextProvider.class)
 */
@Component("AppContextProvider")
@Slf4j
public class AppContextProvider implements ApplicationContextAware {
    @Getter
    private static ApplicationContext appContext;

    @Override
    public void setApplicationContext(ApplicationContext appContext) throws BeansException {
        AppContextProvider.appContext = appContext;
    }

    private static final List<Runnable> shutdownListeners = new ArrayList<>();

    public static void addShutdownListener(Runnable runnable) {
        shutdownListeners.add(runnable);
    }

    public static void removeShutdownListener(Runnable runnable) {
        shutdownListeners.remove(runnable);
    }

    @PreDestroy
    private void shutdown() {
        for (Runnable listener : shutdownListeners) {
            try {
                listener.run();
            } catch (RuntimeException ex) {
                log.error("Shutdown listener failed", ex);
            }
        }
    }
}

