package kafkablocks.function;

/**
 * Бинарный Резолвер
 *
 * @param <TargetPart1> тип первой части "цели"
 * @param <TargetPart2> тип второй части "цели"
 * @param <Result>      тип "результата"
 */
public interface BiResolver<TargetPart1, TargetPart2, Result>
        extends TwoGenericTypeAware<TargetPart1, TargetPart2> {
    /**
     * Выполняет разрешение "цели" до "результата".
     *
     * @param targetPart1 первая часть цели
     * @param targetPart2 вторая часть цели
     * @return результат разрешения.
     */
    Result resolve(TargetPart1 targetPart1, TargetPart2 targetPart2);
}
