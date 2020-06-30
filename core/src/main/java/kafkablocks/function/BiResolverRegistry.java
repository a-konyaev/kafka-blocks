package kafkablocks.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Базовый реестр бинарных резолверов.
 *
 * @param <TargetPart1>
 * @param <TargetPart2>
 * @param <Result>
 */
public abstract class BiResolverRegistry<TargetPart1, TargetPart2, Result> {
    /**
     * Таблица резолверов: класс "цели" -> список резолверов
     */
    private final Map<Integer, List<BiResolver<TargetPart1, TargetPart2, Result>>> resolverMap = new HashMap<>();

    public BiResolverRegistry(List<BiResolverBase<TargetPart1, TargetPart2, Result>> resolvers) {
        resolvers.forEach(resolver ->
                resolverMap
                        .computeIfAbsent(
                                getTargetHashCode(resolver.getFirstTypeClass(), resolver.getSecondTypeClass()),
                                key -> new ArrayList<>())
                        .add(resolver)
        );
    }

    private static int getTargetHashCode(Class<?> targetPart1, Class<?> targetPart2) {
        // реализация позаимствована из кода, сгенеренного lombok-ом
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + targetPart1.hashCode();
        result = result * PRIME + targetPart2.hashCode();
        return result;
    }

    /**
     * Получить резолверы.
     *
     * @param targetPart1 первая часть "цели", для разрешения которой нужно получить резолверы.
     * @param targetPart2 вторая часть "цели"
     * @return коллекция резолверов, которые зарегистрированы для этой цели.
     * @throws IllegalStateException - если не найден ни один резолвер.
     */
    public List<BiResolver<TargetPart1, TargetPart2, Result>> getResolvers(
            TargetPart1 targetPart1, TargetPart2 targetPart2) {

        int targetHashCode = getTargetHashCode(targetPart1.getClass(), targetPart2.getClass());
        List<BiResolver<TargetPart1, TargetPart2, Result>> resolvers = resolverMap.get(targetHashCode);

        if (resolvers == null) {
            throw new NoSuchElementException(String.format("Resolvers for '%s'+'%s' not found",
                    targetPart1.getClass().getName(), targetPart2.getClass().getName()));
        }

        return resolvers;
    }
}
