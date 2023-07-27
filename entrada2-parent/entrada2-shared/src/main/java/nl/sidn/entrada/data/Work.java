package nl.sidn.entrada.data;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class Work {
  
  private String name;
  private String bucket;
  private String key;
  private String server;
  private String location;
  private int size;

}
