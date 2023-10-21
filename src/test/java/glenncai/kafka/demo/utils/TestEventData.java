package glenncai.kafka.demo.utils;

import glenncai.kafka.demo.message.OrderCreated;

import java.util.UUID;

/**
 * Test event data
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
public class TestEventData {

  public static OrderCreated buildOrderCreatedEvent(UUID orderId, String item) {
    return OrderCreated.builder()
                       .orderId(orderId)
                       .item(item)
                       .build();
  }
}
