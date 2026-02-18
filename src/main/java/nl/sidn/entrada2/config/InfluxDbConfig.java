package nl.sidn.entrada2.config;

import java.util.Objects;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.influxdb.v3.client.InfluxDBClient;

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
		return InfluxDBClient.getInstance(Objects.requireNonNull(uri), token.toCharArray(), bucket);
	}

}
