package net.dongliu.direct;

/**
 * @param <T> the value type
 * @author Dongliu
 */
class Value<T> {
    private final T value;
    private final Class type;

    public Value(T value, Class type) {
        this.value = value;
        this.type = type;
    }

    public T getValue() {
        return value;
    }

    public Class getType() {
        return type;
    }

    /**
     * if this value exist(getValue() != null)
     */
    public boolean present() {
        return value != null;
    }
}
