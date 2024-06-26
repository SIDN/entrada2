package nl.sidn.entrada2.metric;

import java.time.Instant;
import java.util.Map;

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

	public SumMetric(String label, int value, Instant time, Map<String, String> tags) {
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
	public int getSamples() {
		return samples;
	}

	public SumMetric(String label, double value, int samples, Instant time, Map<String, String> tags) {
		super();
		this.label = label;
		this.value = value;
		this.samples = samples;
		this.time = time;
		this.tags = tags;
	}

}
