package nl.sidn.entrada2.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;

import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;

@Configuration
@ConditionalOnProperty(prefix = "entrada.metric.influxdb", name = "url", matchIfMissing = false)
public class InfluxDbConfig {

	@Value("${entrada.metrics.influxdb.org}")
	private String influxOrg;
	
	@Value("${entrada.metrics.influxdb.bucket}")
	private String influxBucket;
	
	@Value("${entrada.metrics.influxdb.token}")
	private String influxToken;

	@Value("${entrada.metrics.influxdb.url}")
	private String influxUrl;
	
	@Bean
	public InfluxDBClient influxDbClient() {
		return InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray(), influxOrg, influxBucket);
	}
	
	@Bean
	public WriteApi influxDbWriteApi(InfluxDBClient client) {

		 return client.makeWriteApi(WriteOptions.builder()
                .batchSize(5000)
                .flushInterval(1000)
                .backpressureStrategy(BackpressureOverflowStrategy.DROP_OLDEST)
                .bufferLimit(10000)
                .jitterInterval(1000)
                .retryInterval(5000)
                .build());
                
	}

}
