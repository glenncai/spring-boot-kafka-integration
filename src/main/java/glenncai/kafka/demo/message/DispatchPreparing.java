package glenncai.kafka.demo.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Dispatch preparing message
 *
 * @author Glenn Cai
 * @version 1.0 22/10/2023
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DispatchPreparing {

  UUID orderId;
}
