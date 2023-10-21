package glenncai.kafka.demo.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Dispatch service test
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@SpringBootTest
class DispatchServiceTest {

  @Autowired
  private DispatchService dispatchService;

  @Test
  void testProcess() {
    dispatchService.process("payload");
  }
}