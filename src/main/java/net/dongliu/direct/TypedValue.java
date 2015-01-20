package net.dongliu.direct;

/**
 * value with class info
 *
 * @author Dong Liu
 */
class TypedValue<T> extends Value<T> {
    private final Class type;

    public TypedValue(T value, Class type) {
        super(value);
        this.type = type;
    }

    public Class getType() {
        return type;
    }
}
