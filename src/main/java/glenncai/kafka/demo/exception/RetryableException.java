package glenncai.kafka.demo.exception;

/**
 * Retryable exception
 *
 * @author Glenn Cai
 * @version 1.0 25/10/2023
 */
public class RetryableException extends RuntimeException {

  public RetryableException(String message) {
    super(message);
  }

  public RetryableException(Exception exception) {
    super(exception);
  }
}
