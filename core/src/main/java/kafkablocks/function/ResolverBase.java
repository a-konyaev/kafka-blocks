package kafkablocks.function;

/**
 * Базовый резолвер.
 * @param <Target> Тип "цели" резолва.
 * @param <Result> Тип "результата" резолва.
 */
public abstract class ResolverBase<Target, Result>
        extends OneGenericTypeAwareBase<Target>
        implements Resolver<Target, Result> {
}
