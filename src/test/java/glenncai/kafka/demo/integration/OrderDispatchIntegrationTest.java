package glenncai.kafka.demo.integration;

import static glenncai.kafka.demo.integration.WiremockUtils.stubWiremock;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import glenncai.kafka.demo.config.KafkaConfig;
import glenncai.kafka.demo.message.DispatchPreparing;
import glenncai.kafka.demo.message.OrderCreated;
import glenncai.kafka.demo.message.OrderDispatched;
import glenncai.kafka.demo.utils.TestEventData;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Order dispatch integration test
 *
 * @author Glenn Cai
 * @version 1.0 22/10/2023
 */
@Slf4j
@SpringBootTest(classes = {KafkaConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@AutoConfigureWireMock(port = 0)
@EmbeddedKafka(controlledShutdown = true)
class OrderDispatchIntegrationTest {

  private final static String ORDER_CREATED_TOPIC = "order.created";
  private final static String ORDER_DISPATCHED_TOPIC = "order.dispatched";
  private final static String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
  private final static String ORDER_CREATED_DLT_TOPIC = "order.created.DLT";

  @Resource
  private KafkaTemplate<String, Object> kafkaTemplate;

  @Resource
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Resource
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Resource
  private KafkaTestConsumer kafkaTestConsumer;

  @BeforeEach
  public void setUp() {
    kafkaTestConsumer.dispatchPreparingCounter.set(0);
    kafkaTestConsumer.orderDispatchedCounter.set(0);
    kafkaTestConsumer.orderCreatedDLTCounter.set(0);

    WiremockUtils.reset();

    // Wait until the partitions are assigned.  The application listener container has one topic
    // and the test
    // listener container has multiple topics, so take that into account when awaiting topic
    // assignment.
    kafkaListenerEndpointRegistry.getListenerContainers()
                                 .forEach(
                                     container -> ContainerTestUtils.waitForAssignment(container,
                                                                                       Objects.requireNonNull(
                                                                                           container.getContainerProperties()
                                                                                                    .getTopics()).length *
                                                                                           embeddedKafkaBroker.getPartitionsPerTopic()));
  }

  private void sendMessage(String topic, String key, Object data) throws Exception {
    kafkaTemplate.send(
        MessageBuilder.withPayload(data).setHeader(KafkaHeaders.KEY, key)
                      .setHeader(KafkaHeaders.TOPIC, topic).build()).get();
  }

  @Test
  void test_order_dispatch_flow_success() throws Exception {
    stubWiremock("/api/stock?item=my-item", 200, "true");

    String key = randomUUID().toString();
    OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
    sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

    await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
           .until(kafkaTestConsumer.dispatchPreparingCounter::get, equalTo(1));

    await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
           .until(kafkaTestConsumer.orderDispatchedCounter::get, equalTo(1));
    assertThat(kafkaTestConsumer.orderCreatedDLTCounter.get(), equalTo(0));
  }

  @Test
  void test_order_dispatch_flow_notRetryable_error() throws Exception {
    stubWiremock("api/stock?item=my-item", 400, "Bad request");

    String key = randomUUID().toString();
    OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
    sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

    await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
           .until(kafkaTestConsumer.orderCreatedDLTCounter::get, equalTo(1));
    assertThat(kafkaTestConsumer.dispatchPreparingCounter.get(), equalTo(0));
    assertThat(kafkaTestConsumer.orderDispatchedCounter.get(), equalTo(0));
  }

  @Test
  void test_order_dispatch_flow_retry_then_success() throws Exception {
    stubWiremock("/api/stock?item=my-item", 503, "Service unavailable", "failOnce",
                 Scenario.STARTED, "succeedNextTime");
    stubWiremock("/api/stock?item=my-item", 200, "true", "failOnce", "succeedNextTime",
                 "succeedNextTime");

    String key = randomUUID().toString();
    OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
    sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

    await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
           .until(kafkaTestConsumer.dispatchPreparingCounter::get, equalTo(1));
    await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
           .until(kafkaTestConsumer.orderDispatchedCounter::get, equalTo(1));
    assertThat(kafkaTestConsumer.orderCreatedDLTCounter.get(), equalTo(0));
  }

  @Test
  void test_order_dispatch_flow_retry_util_failure() throws Exception {
    stubWiremock("/api/stock?item=my-item", 503, "Service unavailable");

    String key = randomUUID().toString();
    OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
    sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

    await().atMost(5, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
           .until(kafkaTestConsumer.orderCreatedDLTCounter::get, equalTo(1));
    assertThat(kafkaTestConsumer.dispatchPreparingCounter.get(), equalTo(0));
    assertThat(kafkaTestConsumer.orderDispatchedCounter.get(), equalTo(0));
  }

  @Configuration
  static class TestConfig {
    @Bean
    public KafkaTestConsumer kafkaTestConsumer() {
      return new KafkaTestConsumer();
    }
  }

  /**
   * Use KafkaTestConsumer to consume messages from the output topics
   */
  @KafkaListener(
      groupId = "KafkaIntegrationTest",
      topics = {DISPATCH_TRACKING_TOPIC, ORDER_DISPATCHED_TOPIC, ORDER_CREATED_DLT_TOPIC}
  )
  public static class KafkaTestConsumer {
    AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);
    AtomicInteger orderDispatchedCounter = new AtomicInteger(0);
    AtomicInteger orderCreatedDLTCounter = new AtomicInteger(0);

    @KafkaHandler
    void consumeDispatchTracking(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                 @Payload DispatchPreparing payload) {
      log.debug("Received DispatchPreparing key: {}, message: {}", key, payload);
      assertThat(key, notNullValue());
      assertThat(payload, notNullValue());
      dispatchPreparingCounter.incrementAndGet();
    }

    @KafkaHandler
    void consumeOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                @Payload OrderDispatched payload) {
      log.debug("Received OrderDispatched key: {}, message: {}", key, payload);
      assertThat(key, notNullValue());
      assertThat(payload, notNullValue());
      orderDispatchedCounter.incrementAndGet();
    }

    @KafkaHandler
    void consumeOrderCreatedDLT(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                @Payload OrderCreated payload) {
      log.debug("Received OrderCreatedDLT key: {}, message: {}", key, payload);
      assertThat(key, notNullValue());
      assertThat(payload, notNullValue());
      orderCreatedDLTCounter.incrementAndGet();
    }
  }
}
