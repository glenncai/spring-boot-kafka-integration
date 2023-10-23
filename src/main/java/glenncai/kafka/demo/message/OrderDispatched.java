package glenncai.kafka.demo.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Order dispatched message
 *
 * @author Glenn Cai
 * @version 1.0 21/10/2023
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDispatched {

  UUID orderId;

  UUID processById;

  String notes;
}
