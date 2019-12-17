package kafkablocks;


import lombok.Getter;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * Этот компонент удобно использовать в методах, которые выполняются в треде вне Спринга,
 * но где все таки нужно добраться до контекста.
 * <p>
 * НО чтобы этот компонент был создан спрингом, нужно импортировать его: @Import(AppContextProvider.class)
 */
@Component("AppContextProvider")
public class AppContextProvider implements ApplicationContextAware {
    @Getter
    private static ApplicationContext appContext;

    @Override
    public void setApplicationContext(ApplicationContext appContext) throws BeansException {
        AppContextProvider.appContext = appContext;
    }
}
