package glenncai.kafka.demo.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import glenncai.kafka.demo.exception.RetryableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

/**
 * Stock service client test
 *
 * @author Glenn Cai
 * @version 1.0 25/10/2023
 */
@SpringBootTest
class StockServiceClientTest {

  private static final String STOCK_SERVICE_ENDPOINT = "endpoint";
  private static final String STOCK_SERVICE_QUERY = STOCK_SERVICE_ENDPOINT + "?item=my-item";
  private RestTemplate restTemplateMock;
  private StockServiceClient stockServiceClientMock;

  @BeforeEach
  public void setUp() {
    restTemplateMock = mock(RestTemplate.class);
    stockServiceClientMock = new StockServiceClient(restTemplateMock, STOCK_SERVICE_ENDPOINT);
  }

  @Test
  void test_checkAvailability_success() {
    ResponseEntity<String> response = new ResponseEntity<>("true", HttpStatusCode.valueOf(200));
    when(restTemplateMock.getForEntity(STOCK_SERVICE_QUERY, String.class)).thenReturn(response);
    assertThat(stockServiceClientMock.checkAvailability("my-item"), equalTo("true"));
    verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
  }

  @Test
  void test_checkAvailability_server_error() {
    doThrow(new HttpServerErrorException(HttpStatusCode.valueOf(500)))
        .when(restTemplateMock).getForEntity(STOCK_SERVICE_QUERY, String.class);
    assertThrows(RetryableException.class,
                 () -> stockServiceClientMock.checkAvailability("my-item"));
    verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
  }

  @Test
  void test_checkAvailability_resource_access_error() {
    doThrow(new ResourceAccessException("Access exception")).when(restTemplateMock).getForEntity(
        STOCK_SERVICE_QUERY, String.class);
    assertThrows(RetryableException.class,
                 () -> stockServiceClientMock.checkAvailability("my-item"));
    verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
  }

  @Test
  void test_checkAvailability_runtime_exception() {
    doThrow(new RuntimeException("General exception")).when(restTemplateMock).getForEntity(
        STOCK_SERVICE_QUERY, String.class);
    assertThrows(Exception.class,
                 () -> stockServiceClientMock.checkAvailability("my-item"));
    verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
  }
}