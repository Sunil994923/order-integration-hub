package io.github.sunil994923.orderhub.service;

/** A message that can never be processed successfully; it goes straight to the dead-letter topic. */
public class InvalidOrderException extends RuntimeException {

    public InvalidOrderException(String message) {
        super(message);
    }
}
