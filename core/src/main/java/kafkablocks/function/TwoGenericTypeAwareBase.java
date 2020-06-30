package kafkablocks.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import org.springframework.core.GenericTypeResolver;

public abstract class TwoGenericTypeAwareBase<FirstType, SecondType>
        implements TwoGenericTypeAware<FirstType, SecondType> {

    @Getter
    @JsonIgnore
    private final Class<FirstType> firstTypeClass;
    @Getter
    @JsonIgnore
    private final Class<SecondType> secondTypeClass;

    @SuppressWarnings("unchecked")
    protected TwoGenericTypeAwareBase() {
        Class<?>[] types = GenericTypeResolver.resolveTypeArguments(getClass(), TwoGenericTypeAwareBase.class);
        assert types != null;

        this.firstTypeClass = (Class<FirstType>) types[0];
        this.secondTypeClass = (Class<SecondType>) types[1];
    }
}
