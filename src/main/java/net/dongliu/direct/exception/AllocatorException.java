package net.dongliu.direct.exception;

public class AllocatorException extends Exception {
    public AllocatorException() {
    }

    public AllocatorException(String message) {
        super(message);
    }

    public AllocatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public AllocatorException(Throwable cause) {
        super(cause);
    }
}
