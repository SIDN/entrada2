package nl.sidn.entrada2.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;

import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "management.influx.metrics.export")
@ConditionalOnExpression(
	    "T(org.apache.commons.lang3.StringUtils).isNotEmpty('${management.influx.metrics.export.uri}')"
	)
public class InfluxDbConfig {

	private String org;
	private String bucket;
	private String token;
	private String uri;
	
	@Bean
	public InfluxDBClient influxDbClient() {
		return InfluxDBClientFactory.create(uri, token.toCharArray(), org, bucket);
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
