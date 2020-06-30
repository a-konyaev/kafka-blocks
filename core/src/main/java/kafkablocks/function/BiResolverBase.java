package kafkablocks.function;

/**
 * Базовый бинарный резолвер.
 *
 * @param <TargetPart1> тип первой части "цели" резолва.
 * @param <TargetPart2> тип второй части "цели" резолва.
 * @param <Result>      Тип "результата" резолва.
 */
public abstract class BiResolverBase<TargetPart1, TargetPart2, Result>
        extends TwoGenericTypeAwareBase<TargetPart1, TargetPart2>
        implements BiResolver<TargetPart1, TargetPart2, Result> {
}
