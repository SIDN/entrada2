package nl.sidn.entrada2.api.data;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class WorkResult {
  
  private long id;
  private String worker;
  private String filename;
  private long time;
  private List<byte[]> dataFiles;

}
