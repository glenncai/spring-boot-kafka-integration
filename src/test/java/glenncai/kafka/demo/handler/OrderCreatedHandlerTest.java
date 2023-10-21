package glenncai.kafka.demo.handler;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import glenncai.kafka.demo.message.OrderCreated;
import glenncai.kafka.demo.service.DispatchService;
import glenncai.kafka.demo.utils.TestEventData;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Order created handler test
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@SpringBootTest
class OrderCreatedHandlerTest {

  @Mock
  private DispatchService dispatchServiceMock;

  @InjectMocks
  private OrderCreatedHandler orderCreatedHandlerMock;

  @Test
  void testListen() {
    OrderCreated testEvent =
        TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());

    orderCreatedHandlerMock.listen(testEvent);
    verify(dispatchServiceMock, times(1)).process(testEvent);
  }
}