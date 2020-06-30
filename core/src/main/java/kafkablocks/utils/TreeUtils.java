package kafkablocks.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TreeUtils {
    private TreeUtils() {
    }

    /**
     * Строит дерево из эл-тов items, при этом связи между узлами определены в childParentMap
     *
     * @param childParentMap таблица связей {Ключ ребенка -> Ключ родителя}
     * @param items          список эл-тов, где каждый эл-т - это пара (Ключ, Значение)
     * @param <K>            тип ключа
     * @param <V>            тип значения
     * @return список корневых эл-тов дерева
     */
    public static <K, V> List<TreeNode<Map.Entry<K, V>>> buildTree(
            Set<Map.Entry<K, V>> items,
            Map<K, K> childParentMap) {

        Map<K, TreeNode<Map.Entry<K, V>>> nodeMap = new HashMap<>();

        // сначала создадим узлы, которые еще не связаны между собой,
        // но при этом сформируем таблицу: {Ключ узла -> узел}
        for (Map.Entry<K, V> item : items) {
            nodeMap.put(item.getKey(), new TreeNode<>(item));
        }

        List<TreeNode<Map.Entry<K, V>>> roots = new ArrayList<>();

        // теперь пройдемся по всем узлам и свяжем их, используя childParentMap
        for (Map.Entry<K, TreeNode<Map.Entry<K, V>>> nodeEntry : nodeMap.entrySet()) {
            K key = nodeEntry.getKey();
            TreeNode<Map.Entry<K, V>> node = nodeEntry.getValue();
            K parentKey = childParentMap.get(key);

            if (parentKey != null) {
                TreeNode<Map.Entry<K, V>> parentNode = nodeMap.get(parentKey);
                if (parentNode == null) {
                    throw new IllegalStateException(String.format(
                            "Parent node undefined! child key = '%s'; parent key = '%s'", key, parentKey));
                }

                parentNode.addChild(node);
            }
            else {
                // если parentKey == NULL, значит это корневой узел
                roots.add(node);
            }
        }

        return roots;
    }
}
