package kafkablocks;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafkablocks.utils.ClassUtils;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.util.List;


/**
 * Базовый класс для параметров приложения.
 * Вызывает инициализацию у классов наследников и распечатывать в лог значения параметров.
 */
public abstract class AppProperties {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @SneakyThrows
    @PostConstruct
    private void _init() {
        init();

        // todo: если данный класс унаследует другой, у кот. висит аннотация @Configuration,
        // то здесь мы получим прокси-класс CGLIB-чего-то-там.
        // А если у класса-наследника аннотация @Component, то все красиво.
        // как вариант: можно смотреть на имя класса - если в нем есть $$, то это прокси и надо взять super-класс
        Class<? extends AppProperties> type = this.getClass();

        StringBuilder sb = new StringBuilder(1024);
        sb.append(type.getSimpleName());
        sb.append(" values:");

        List<Field> fields = ClassUtils.getFields(type, AppProperties.class, false);
        for (Field field : fields) {
            sb.append("\n\t");
            sb.append(field.getName());
            sb.append(" = ");
            field.setAccessible(true);
            sb.append(field.get(this));

            //todo: Map-ы и печатать с переносом строк
        }

        logger.info(sb.toString());
    }

    protected void init() {
    }
}
