package kafkablocks.function;

/**
 * Осведомленный о двух первых дженерик-типах
 *
 * @param <FirstType>  первый дженерик-тип
 * @param <SecondType> второй   дженерик-тип
 */
public interface TwoGenericTypeAware<FirstType, SecondType> {
    Class<FirstType> getFirstTypeClass();

    Class<SecondType> getSecondTypeClass();
}
