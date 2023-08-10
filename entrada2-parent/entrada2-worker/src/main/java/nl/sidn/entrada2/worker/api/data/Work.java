package nl.sidn.entrada2.worker.api.data;

import lombok.Builder;
import lombok.Value;
import nl.sidn.entrada2.worker.service.StateService.APP_STATE;

@Builder
@Value
public class Work {
  
  private long id;
  private String name;
  private String bucket;
  private String key;
  private String server;
  private String location;
  private int size;
  
  private APP_STATE state;

}
