package glenncai.kafka.demo.client;

import glenncai.kafka.demo.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

/**
 * Mock stock service client
 *
 * @author Glenn Cai
 * @version 1.0 25/10/2023
 */
@Slf4j
@Component
public class StockServiceClient {

  private final RestTemplate restTemplate;

  private final String stockServiceEndpoint;

  public StockServiceClient(@Autowired RestTemplate restTemplate,
                            @Value("${dispatch.stockServiceEndpoint}")
                            String stockServiceEndpoint) {
    this.restTemplate = restTemplate;
    this.stockServiceEndpoint = stockServiceEndpoint;
  }

  /**
   * Check if the item is available in stock service
   *
   * @param item item
   * @return Boolean true if available
   */
  public String checkAvailability(String item) {
    try {
      ResponseEntity<String> response =
          restTemplate.getForEntity(stockServiceEndpoint + "?item=" + item, String.class);
      if (response.getStatusCode().value() != 200) {
        throw new RuntimeException("Error " + response.getStatusCode().value());
      }
      return response.getBody(); // "true" or "false"
    } catch (HttpServerErrorException | ResourceAccessException e) {
      log.warn("Failure calling stock service", e);
      throw new RetryableException(e);
    } catch (Exception e) {
      log.error("Exception thrown: " + e.getClass().getName(), e);
      throw e;
    }
  }
}
