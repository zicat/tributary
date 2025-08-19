package org.logstash.beats;

/** InvalidFrameProtocolException. */
public class InvalidFrameProtocolException extends RuntimeException {
    InvalidFrameProtocolException(String message) {
        super(message);
    }
}
