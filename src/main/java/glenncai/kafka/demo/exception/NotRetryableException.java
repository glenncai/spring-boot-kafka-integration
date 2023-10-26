package glenncai.kafka.demo.exception;

/**
 * Not retryable exception
 *
 * @author Glenn Cai
 * @version 1.0 25/10/2023
 */
public class NotRetryableException extends RuntimeException {

  public NotRetryableException(String message) {
    super(message);
  }

  public NotRetryableException(Exception exception) {
    super(exception);
  }
}
