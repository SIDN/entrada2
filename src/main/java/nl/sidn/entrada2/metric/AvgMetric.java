package nl.sidn.entrada2.metric;

import java.util.Map;

import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Getter
public class AvgMetric extends SumMetric {

	private long min;
	private long max;

	public AvgMetric(int value, Map<String, String> labels) {
		super(value, labels);
	}
	
	@Override
	public double getValue() {

		if (samples > 0) {
			return value / samples;
		}
		return 0;
	}

	@Override
	public void update(int value) {
		super.update(value);
		if(value > max) {
			max = value;
		}else if(value < min){
			min = value;
		}
	}

	@Override
	public String toString() {
		return "AvgMetric [min=" + min + ", max=" + max + "]";
	}

}
