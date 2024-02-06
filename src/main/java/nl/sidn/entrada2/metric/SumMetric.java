package nl.sidn.entrada2.metric;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class SumMetric implements Metric {

  protected String label;
  protected double value;
  protected int samples;
  protected Instant time;
  protected Map<String, String> tags;


  public SumMetric(String label, int value, Instant time,  Map<String, String> tags) {
    this.label = label;
    this.time = time;
    this.tags = tags;
    update(value);
  }

  public void update(int value) {
    this.value += value;
    samples++;
  }

  @Override
  public String toString() {
    return label + " " + value + " " + time;
  }

  @Override
  public int getSamples() {
    return samples;
  }



}
