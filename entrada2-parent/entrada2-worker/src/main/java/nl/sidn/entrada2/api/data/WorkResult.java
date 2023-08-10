package nl.sidn.entrada2.api.data;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class WorkResult {
  
  private long id;
  private long time;
  private long rows;

}
