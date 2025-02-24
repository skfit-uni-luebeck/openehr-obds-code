package de.uksh.medic.etl;

public class ProcessingException extends RuntimeException {
    public ProcessingException(String message) {
        super(message);
    }

    public ProcessingException(Throwable cause) {
        super(cause);
    }

    public ProcessingException() {
    }
}
