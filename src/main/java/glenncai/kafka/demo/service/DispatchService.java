package glenncai.kafka.demo.service;

import glenncai.kafka.demo.message.OrderCreated;
import glenncai.kafka.demo.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

/**
 * Dispatch service
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@Service
@RequiredArgsConstructor
public class DispatchService {

  private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
  private final KafkaTemplate<String, Object> kafkaTemplate;

  public void process(OrderCreated orderCreated) throws ExecutionException, InterruptedException {
    OrderDispatched orderDispatched = OrderDispatched.builder()
                                                     .orderId(orderCreated.getOrderId())
                                                     .build();
    // The call to get() on it makes the send synchronous
    kafkaTemplate.send(ORDER_DISPATCHED_TOPIC, orderDispatched).get();
  }
}
