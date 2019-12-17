package kafkablocks.utils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class ClassUtilsTest {
    @Test
    public void getFields_0() {
        List<Field> fields = ClassUtils.getFields(T1.class, T1.class, false);
        Assert.assertEquals(0, fields.size());
    }

    @Test
    public void getFields_1() {
        Set<String> actual = getFieldNames(T1.class, T1.class, true);
        String[] expected = {"this$0", "f1", "f2"};
        Assert.assertTrue(actual.containsAll(Arrays.asList(expected)));
    }

    @Test
    public void getFields_2() {
        Set<String> actual = getFieldNames(T3.class, T1.class, true);
        String[] expected = {"this$0", "f1", "f2", "f3", "f4", "f5", "f6"};
        Assert.assertTrue(actual.containsAll(Arrays.asList(expected)));
    }

    @Test
    public void getFields_3() {
        Set<String> actual = getFieldNames(T3.class, T2.class, true);
        String[] expected = {"this$0", "f3", "f4", "f5", "f6"};
        Assert.assertTrue(actual.containsAll(Arrays.asList(expected)));
    }

    @Test
    public void getFields_4() {
        Set<String> actual = getFieldNames(T3.class, T1.class, false);
        String[] expected = {"this$0", "f3", "f4", "f5", "f6"};
        Assert.assertTrue(actual.containsAll(Arrays.asList(expected)));
    }

    @SuppressWarnings("unchecked")
    private Set<String> getFieldNames(Class type, Class toBaseType, boolean includeBase) {
        List<Field> fields = (List<Field>)ClassUtils.getFields(type, toBaseType, includeBase);
        return fields.stream().map(Field::getName).collect(Collectors.toSet());
    }

    class T1 {
        private int f1;
        public int f2;
    }

    class T2 extends T1 {
        private int f3;
        public int f4;
    }

    class T3 extends T2 {
        private int f5;
        public int f6;
    }
}
