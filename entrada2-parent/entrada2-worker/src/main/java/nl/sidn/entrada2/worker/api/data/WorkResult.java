package nl.sidn.entrada2.worker.api.data;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class WorkResult {
  
  private long id;
  private long time;
  private long rows;

}
