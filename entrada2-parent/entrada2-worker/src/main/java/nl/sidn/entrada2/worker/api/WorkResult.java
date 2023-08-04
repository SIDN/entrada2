package nl.sidn.entrada2.worker.api;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class WorkResult {
  
  private long id;
  private long time;
  private long rows;

}
