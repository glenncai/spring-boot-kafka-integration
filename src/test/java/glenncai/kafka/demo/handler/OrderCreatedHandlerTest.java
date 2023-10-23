package glenncai.kafka.demo.handler;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
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
  void test_listen_success() throws Exception {
    String key = randomUUID().toString();
    OrderCreated testEvent =
        TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());

    orderCreatedHandlerMock.listen(0, key, testEvent);
    verify(dispatchServiceMock, times(1)).process(key, testEvent);
  }

  @Test
  void test_listen_service_failure() throws Exception {
    String key = randomUUID().toString();
    OrderCreated testEvent =
        TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());

    doThrow(new RuntimeException("Service failure")).when(dispatchServiceMock)
                                                    .process(key, testEvent);

    Exception exception = assertThrows(RuntimeException.class, () -> {
      dispatchServiceMock.process(key, testEvent);
    });

    verify(dispatchServiceMock, times(1)).process(key, testEvent);
    assertThat(exception.getMessage()).isEqualTo("Service failure");
  }
}