package net.dongliu.direct.struct;

/**
 * @param <T> the value type
 * @author Dongliu
 */
public class Value<T> {
    private final T value;

    public Value(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
