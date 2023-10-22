package glenncai.kafka.demo.service;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import glenncai.kafka.demo.message.DispatchPreparing;
import glenncai.kafka.demo.message.OrderCreated;
import glenncai.kafka.demo.message.OrderDispatched;
import glenncai.kafka.demo.utils.TestEventData;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

/**
 * Dispatch service test
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@SpringBootTest
class DispatchServiceTest {

  @Mock
  private KafkaTemplate<String, Object> kafkaTemplateMock;

  @InjectMocks
  private DispatchService dispatchServiceMock;

  @Test
  void test_process_producer_success() throws Exception {
    when(kafkaTemplateMock.send(anyString(), any(DispatchPreparing.class))).thenAnswer(
        invocation -> CompletableFuture.completedFuture(null));
    when(kafkaTemplateMock.send(anyString(), any(OrderDispatched.class))).thenAnswer(
        invocation -> CompletableFuture.completedFuture(null));

    OrderCreated testEvent =
        TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());

    dispatchServiceMock.process(testEvent);

    verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), any(DispatchPreparing.class));
    verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), any(OrderDispatched.class));
  }

  @Test
  void test_process_dispatch_tracking_failure() {
    OrderCreated testEvent =
        TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());

    doThrow(new RuntimeException("Dispatch tracking producer failure")).when(kafkaTemplateMock)
                                                                       .send(
                                                                           eq("dispatch.tracking"),
                                                                           any(DispatchPreparing.class));
    Exception exception =
        assertThrows(RuntimeException.class, () -> dispatchServiceMock.process(testEvent));

    verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), any(DispatchPreparing.class));
    verifyNoMoreInteractions(kafkaTemplateMock);
    assertThat(exception.getMessage()).isEqualTo("Dispatch tracking producer failure");
  }

  @Test
  void test_process_order_dispatched_failure() {
    OrderCreated testEvent =
        TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
    when(kafkaTemplateMock.send(anyString(), any(DispatchPreparing.class))).thenAnswer(
        invocation -> CompletableFuture.completedFuture(null));

    doThrow(new RuntimeException("Order dispatched producer failure")).when(kafkaTemplateMock)
                                                                      .send(
                                                                          eq("order.dispatched"),
                                                                          any(OrderDispatched.class));
    Exception exception =
        assertThrows(RuntimeException.class, () -> dispatchServiceMock.process(testEvent));

    verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), any(DispatchPreparing
                                                                              .class));
    verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), any(OrderDispatched.class));
    assertThat(exception.getMessage()).isEqualTo("Order dispatched producer failure");
  }
}