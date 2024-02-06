package nl.sidn.entrada2.config;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class S3Config {

	@Value("${entrada.s3.region}")
	private String region;

	@Value("${entrada.s3.endpoint}")
	private String endpoint;

	@Bean
	@ConditionalOnProperty(value = "entrada.s3.endpoint", matchIfMissing = true)
	S3Client s3() {

		if (StringUtils.isBlank(endpoint)) {
			return S3Client.builder().region(Region.of(region)).forcePathStyle(Boolean.TRUE).build();
		}

		return S3Client.builder().region(Region.of(region)).forcePathStyle(Boolean.TRUE)
				.endpointOverride(URI.create(endpoint)).build();

	}

}
