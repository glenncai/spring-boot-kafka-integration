package glenncai.kafka.demo.handler;

import glenncai.kafka.demo.message.OrderCreated;
import glenncai.kafka.demo.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Order created handler for Kafka consumer
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@RequiredArgsConstructor
@Component
@Slf4j
public class OrderCreatedHandler {

  private final DispatchService dispatchService;

  @KafkaListener(
      id = "orderConsumerClient",
      topics = "order.created",
      groupId = "dispatch.order.created.consumer",
      containerFactory = "kafkaListenerContainerFactory"
  )
  public void listen(OrderCreated payload) {
    log.info("Received message: {}", payload);
    try {
      dispatchService.process(payload);
    } catch (Exception e) {
      log.error("Processing failed: {}", e.getMessage());
    }
  }
}
