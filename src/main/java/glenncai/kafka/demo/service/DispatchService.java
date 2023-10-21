package glenncai.kafka.demo.service;

import glenncai.kafka.demo.message.OrderCreated;
import org.springframework.stereotype.Service;

/**
 * Dispatch service
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@Service
public class DispatchService {

  public void process(OrderCreated payload) {
    // no-op
  }
}
