package kafkablocks.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import org.springframework.core.GenericTypeResolver;

public abstract class OneGenericTypeAwareBase<FirstType>
        implements OneGenericTypeAware<FirstType> {

    @Getter
    @JsonIgnore
    private final Class<FirstType> firstTypeClass;

    @SuppressWarnings("unchecked")
    protected OneGenericTypeAwareBase() {
        Class<?>[] types = GenericTypeResolver.resolveTypeArguments(getClass(), OneGenericTypeAwareBase.class);
        assert types != null;

        this.firstTypeClass = (Class<FirstType>) types[0];
    }
}
