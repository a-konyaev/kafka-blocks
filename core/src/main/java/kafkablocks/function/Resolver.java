package kafkablocks.function;

/**
 * Резолвер
 *
 * @param <Target> тип "цели"
 * @param <Result> тип "результата"
 */
public interface Resolver<Target, Result> extends OneGenericTypeAware<Target> {
    /**
     * Выполняет разрешение "цели" до "результата".
     *
     * @param target цель
     * @return результат разрешения.
     */
    Result resolve(Target target);
}
