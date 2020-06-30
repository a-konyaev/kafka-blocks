package kafkablocks.function;

/**
 * Осведомленный об одном (первом) дженерик-типе
 *
 * @param <FirstType> первый дженерик-тип
 */
public interface OneGenericTypeAware<FirstType> {
    Class<FirstType> getFirstTypeClass();
}
