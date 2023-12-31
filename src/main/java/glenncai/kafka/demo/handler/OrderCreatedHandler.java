package glenncai.kafka.demo.handler;

import glenncai.kafka.demo.exception.NotRetryableException;
import glenncai.kafka.demo.exception.RetryableException;
import glenncai.kafka.demo.message.OrderCreated;
import glenncai.kafka.demo.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Order created handler for Kafka consumer
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreatedHandler {

  private final DispatchService dispatchService;

  @KafkaListener(
      id = "orderConsumerClient",
      topics = "order.created",
      groupId = "dispatch.order.created.consumer",
      containerFactory = "kafkaListenerContainerFactory"
  )
  public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                     @Header(KafkaHeaders.RECEIVED_KEY) String key,
                     @Payload OrderCreated payload) {
    log.info("Received message: partition={}, key={}, payload={}", partition, key, payload);
    try {
      dispatchService.process(key, payload);
    } catch (RetryableException e) {
      log.warn("Retryable exception: " + e.getMessage());
      throw e;
    } catch (Exception e) {
      log.error("NotRetryable exception: " + e.getMessage());
      throw new NotRetryableException(e);
    }
  }
}
