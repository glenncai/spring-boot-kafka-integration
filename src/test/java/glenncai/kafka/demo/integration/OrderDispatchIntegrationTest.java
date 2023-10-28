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

    WiremockUtils.reset();

    // Wait until the partitions are assigned
    kafkaListenerEndpointRegistry.getListenerContainers()
                                 .forEach(
                                     container -> ContainerTestUtils.waitForAssignment(container,
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

    await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
           .until(kafkaTestConsumer.orderDispatchedCounter::get, equalTo(1));
  }

  @Test
  void test_order_dispatch_flow_notRetryable_error() throws Exception {
    stubWiremock("api/stock?item=my-item", 400, "Bad request");

    String key = randomUUID().toString();
    OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
    sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

    TimeUnit.SECONDS.sleep(3);

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
  public static class KafkaTestConsumer {
    AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);
    AtomicInteger orderDispatchedCounter = new AtomicInteger(0);

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
    void consumeDispatchTracking(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                 @Payload DispatchPreparing payload) {
      log.debug("Received DispatchPreparing key: {}, message: {}", key, payload);
      assertThat(key, notNullValue());
      assertThat(payload, notNullValue());
      dispatchPreparingCounter.incrementAndGet();
    }

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = ORDER_DISPATCHED_TOPIC)
    void consumeOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                @Payload OrderDispatched payload) {
      log.debug("Received OrderDispatched key: {}, message: {}", key, payload);
      assertThat(key, notNullValue());
      assertThat(payload, notNullValue());
      orderDispatchedCounter.incrementAndGet();
    }
  }
}
