package glenncai.kafka.demo.service;

import static java.util.UUID.randomUUID;
import glenncai.kafka.demo.client.StockServiceClient;
import glenncai.kafka.demo.message.DispatchPreparing;
import glenncai.kafka.demo.message.OrderCreated;
import glenncai.kafka.demo.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Dispatch service
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService {

  private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
  private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
  private static final UUID APPLICATION_ID = randomUUID();
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final StockServiceClient stockServiceClient;

  public void process(String key, OrderCreated orderCreated)
      throws ExecutionException, InterruptedException {

    String stockAvailable = stockServiceClient.checkAvailability(orderCreated.getItem());

    if (Boolean.parseBoolean(stockAvailable)) {
      DispatchPreparing dispatchPreparing = DispatchPreparing.builder()
                                                             .orderId(orderCreated.getOrderId())
                                                             .build();
      // The call to get() on it makes the send synchronous
      kafkaTemplate.send(DISPATCH_TRACKING_TOPIC, key, dispatchPreparing).get();

      OrderDispatched orderDispatched = OrderDispatched.builder()
                                                       .orderId(orderCreated.getOrderId())
                                                       .processById(APPLICATION_ID)
                                                       .notes(
                                                           "Dispatched: " + orderCreated.getItem())
                                                       .build();
      // The call to get() on it makes the send synchronous
      kafkaTemplate.send(ORDER_DISPATCHED_TOPIC, key, orderDispatched).get();

      log.info("Sent message: key: {}, orderId: {}, processById: {}, notes: {}",
               key,
               orderDispatched.getOrderId(),
               orderDispatched.getProcessById(), orderDispatched.getNotes());
    } else {
      log.info("Stock not available for item: {}", orderCreated.getItem());
    }
  }
}
