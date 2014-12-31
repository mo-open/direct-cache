package net.dongliu.direct.exception;

/**
 * @author Dong Liu dongliu@live.cn
 */
public class IllegalReferenceCountException extends RuntimeException {
    public IllegalReferenceCountException() {
    }

    public IllegalReferenceCountException(int refCnt) {
        this("refCnt: " + refCnt);
    }

    public IllegalReferenceCountException(int refCnt, int increment) {
        this("refCnt: " + refCnt + ", " + (increment > 0 ? "increment: " + increment : "decrement: " + -increment));
    }

    public IllegalReferenceCountException(String message) {
        super(message);
    }

    public IllegalReferenceCountException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalReferenceCountException(Throwable cause) {
        super(cause);
    }
}
