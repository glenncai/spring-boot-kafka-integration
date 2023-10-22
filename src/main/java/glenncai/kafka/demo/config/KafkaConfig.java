package glenncai.kafka.demo.config;

import glenncai.kafka.demo.message.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@ComponentScan(basePackages = "glenncai.kafka.demo")
@PropertySource(value = "classpath:application.yml")
@Configuration
public class KafkaConfig {

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
      ConsumerFactory<String, Object> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    return factory;
  }

  @Bean
  public ConsumerFactory<String, Object> consumerFactory(
      @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
    config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreated.class.getCanonicalName());
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return new DefaultKafkaConsumerFactory<>(config);
  }

  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate(
      ProducerFactory<String, Object> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  public ProducerFactory<String, Object> producerFactory(
      @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    return new DefaultKafkaProducerFactory<>(config);
  }
}
