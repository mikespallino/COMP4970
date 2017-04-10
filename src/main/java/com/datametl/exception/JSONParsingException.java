package com.datametl.exception;

/**
 * Created by TseAndy on 3/1/17.
 */
public class JSONParsingException extends Exception{

    /**
     * Empty Constructor
     */
    public JSONParsingException() {

    }

    /**
     * Constructs an Exception with string
     *
     * @param message message
     */
    public JSONParsingException(String message) {

        super(message);
    }

    /**
     * Constructs an Exception with message and cause
     *
     * @param message String
     * @param cause cause
     */
    public JSONParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an Exception with cause
     *
     * @param cause cause
     */
    public JSONParsingException(Throwable cause) {
        super(cause);
    }
}
