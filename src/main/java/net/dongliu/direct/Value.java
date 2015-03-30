package net.dongliu.direct;

/**
 * Cache value entry.
 *
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
