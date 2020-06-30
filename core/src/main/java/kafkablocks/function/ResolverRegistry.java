package kafkablocks.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Базовый реестр резолверов.
 *
 * @param <Target>
 * @param <Result>
 */
public abstract class ResolverRegistry<Target, Result> {
    /**
     * Таблица резолверов: класс "цели" -> список резолверов
     */
    private final Map<Class<? extends Target>, List<Resolver<Target, Result>>> resolverMap = new HashMap<>();

    public ResolverRegistry(List<ResolverBase<Target, Result>> resolvers) {
        resolvers.forEach(resolver ->
                resolverMap
                        .computeIfAbsent(resolver.getFirstTypeClass(), key -> new ArrayList<>())
                        .add(resolver)
        );
    }

    /**
     * Получить резолверы.
     *
     * @param target "цель", для разрешения которой нужно получить резолверы.
     * @return коллекция резолверов, которые зарегистрированы для этой цели.
     * @throws IllegalStateException - если не найден ни один резолвер.
     */
    public List<Resolver<Target, Result>> getResolvers(Target target) {
        Class<?> targetClass = target.getClass();
        List<Resolver<Target, Result>> resolvers = resolverMap.get(targetClass);

        if (resolvers == null) {
            throw new NoSuchElementException(String.format("Resolvers for '%s' not found", targetClass.getName()));
        }

        return resolvers;
    }
}
