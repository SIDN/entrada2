package nl.sidn.entrada2.worker.api;

import lombok.Builder;
import lombok.Value;

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

}
