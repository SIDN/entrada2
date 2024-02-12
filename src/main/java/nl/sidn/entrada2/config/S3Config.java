package nl.sidn.entrada2.config;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.EventBridgeConfiguration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;

@Configuration
public class S3Config {

	@Value("${entrada.s3.endpoint}")
	private String endpoint;

	@Value("${entrada.s3.bucket}")
	private String bucketName;

	private boolean isRunningOnAws() {
		return StringUtils.isBlank(endpoint);
	}

	@Bean
	// @ConditionalOnProperty(value = "entrada.s3.endpoint", matchIfMissing = true)
	public S3Client s3() {

		if (isRunningOnAws()) {
			// .region(Region.of(region))
			return S3Client.builder().forcePathStyle(Boolean.TRUE).build();
		}
		return S3Client.builder().forcePathStyle(Boolean.TRUE).endpointOverride(URI.create(endpoint)).build();
	}

	@PostConstruct
	private void init() {
		if (!isBucketExist(s3(), bucketName)) {
			createBucket(s3(), bucketName, isRunningOnAws());
		}
	}

	private boolean createBucket(S3Client s3Client, String bucket, boolean enableEventBridge) {

		CreateBucketRequest bucketRequest = CreateBucketRequest.builder().bucket(bucket).build();

		CreateBucketResponse r = s3Client.createBucket(bucketRequest);

		if (enableEventBridge && r.sdkHttpResponse().isSuccessful()) {

			EventBridgeConfiguration bridge = EventBridgeConfiguration.builder().build();
			NotificationConfiguration configuration = NotificationConfiguration.builder()
					.eventBridgeConfiguration(bridge).build();

			PutBucketNotificationConfigurationRequest configurationRequest = PutBucketNotificationConfigurationRequest
					.builder().bucket(bucket).notificationConfiguration(configuration).skipDestinationValidation(true)
					.build();

			s3Client.putBucketNotificationConfiguration(configurationRequest);
		}

		return r.sdkHttpResponse().isSuccessful();

	}

	private boolean isBucketExist(S3Client s3Client, String bucket) {
		HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucket).build();

		try {
			s3Client.headBucket(headBucketRequest);
			return true;
		} catch (NoSuchBucketException e) {
			return false;
		}
	}
}
