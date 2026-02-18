package nl.sidn.entrada2.metric;

import java.util.Map;

public interface Metric {

  void update(int value);

  /**
   * sample-size for calculated value
   * 
   * @return sample-size, -1 if value is based on single sample
   */
  int getSamples();

  double getValue();

  Map<String, String> getTags();
  
  String getServer();
  
  String getAnycastSite();

}
