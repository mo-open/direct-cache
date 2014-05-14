package net.dongliu.direct.exception;

/**
 * @author dongliu
 */
public class DeSerializeException extends Exception {
    public DeSerializeException() {
    }

    public DeSerializeException(String message) {
        super(message);
    }

    public DeSerializeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeSerializeException(Throwable cause) {
        super(cause);
    }
}
