package net.dongliu.direct.exception;

public final class SerializerNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 5095679349348496962L;


    public SerializerNotFoundException(Exception e) {
        super(e);
    }
}
