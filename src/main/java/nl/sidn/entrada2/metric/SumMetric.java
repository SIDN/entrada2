package nl.sidn.entrada2.metric;

import java.util.Map;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class SumMetric implements Metric {

	protected double value;
	protected int samples;
	protected Map<String, String> tags;
	protected String server;
	protected String anycastSite;

	public SumMetric(int value, Map<String, String> tags, String server, String anycastSite) {
		this.tags = tags;
		this.server = server;
		this.anycastSite = anycastSite;
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

	public SumMetric(String label, double value, int samples, Map<String, String> tags, String server, String anycastSite) {
		this.value = value;
		this.samples = samples;
		this.tags = tags;
		this.server = server;
		this.anycastSite = anycastSite;
	}

}
