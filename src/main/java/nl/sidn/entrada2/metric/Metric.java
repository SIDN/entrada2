package nl.sidn.entrada2.metric;

import java.time.Instant;
import java.util.Map;

public interface Metric {

  void update(int value);

  /**
   * sample-size for calculated value
   * 
   * @return sample-size, -1 if value is based on single sample
   */
  int getSamples();

  Instant getTime();
  
  void setTime(Instant time);
  
  String getLabel();

  double getValue();

  Map<String, String> getTags();

}
