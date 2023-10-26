package glenncai.kafka.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;

@SpringBootTest
@PropertySource(value = "classpath:application.yml")
class KafkaExampleApplicationTests {

  @Value("${dispatch.stockServiceEndpoint}")
  private String stockServiceEndpoint;

  @Test
  void contextLoads() {
    assertEquals("http://localhost:9001/api/stock", stockServiceEndpoint);
  }

}
