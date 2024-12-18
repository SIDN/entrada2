package nl.sidn.entrada2.config;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.EventBridgeConfiguration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;

@Configuration
@Slf4j
public class S3Config {
	
	@Value("${entrada.provisioning.enabled:true}")
	private boolean provisioningEnabled;

	@Value("${entrada.s3.endpoint}")
	private String endpoint;

	@Value("${entrada.s3.bucket}")
	private String bucketName;

	private boolean isRunningOnAws() {
		return StringUtils.isBlank(endpoint);
	}

	@Bean
	public S3Client s3() {

		if (isRunningOnAws()) {
			return S3Client.builder().forcePathStyle(Boolean.TRUE).build();
		}
		// when not running on aws, make sure the s2 endpoint is configured
		 return S3Client.builder().forcePathStyle(Boolean.TRUE).endpointOverride(URI.create(endpoint))
				.build();
	}

	@PostConstruct
	private void init() {
		log.info("Using s3 endpoint: {}", endpoint);
		log.info("Using s3 bucket: {}", bucketName);
		
		if(!provisioningEnabled) {
			log.info("Provisioning is disabled, do not create required bucket");
			return;
		}

		if (!isBucketExist(s3(), bucketName)) {
			createBucket(s3(), bucketName, isRunningOnAws());
		}
	}

	private boolean createBucket(S3Client s3Client, String bucket, boolean enableEventBridge) {

		boolean bucketOk = createBucket(s3Client, bucket);
		if (enableEventBridge && bucketOk) {
			createBucketNotification(s3Client, bucket);			
		}

		return bucketOk;

	}
	
	public void createBucketNotification(S3Client s3Client, String bucket) {
		EventBridgeConfiguration bridge = EventBridgeConfiguration.builder().build();
		NotificationConfiguration configuration = NotificationConfiguration.builder()
				.eventBridgeConfiguration(bridge).build();

		PutBucketNotificationConfigurationRequest configurationRequest = PutBucketNotificationConfigurationRequest
				.builder().bucket(bucket).notificationConfiguration(configuration)
				.skipDestinationValidation(true)
				.build();

		try {
			s3Client.putBucketNotificationConfiguration(configurationRequest);
		} catch (Exception e) {
			log.error("Creating bucket notification failed");
		}
	}
	
	public boolean createBucket(S3Client s3Client, String bucket) {
		log.info("Create bucket: {}", bucket);

		try {
			CreateBucketRequest bucketRequest = CreateBucketRequest.builder().bucket(bucket).build();
			CreateBucketResponse r = s3Client.createBucket(bucketRequest);
			return r.sdkHttpResponse().isSuccessful();
		} catch (BucketAlreadyOwnedByYouException e) {
			log.info("Create bucket operation failed because bucket already exists and is owned by you");
			return true;
		} catch (Exception e) {
			log.error("Create bucket operation failed for: " + bucket, e);			
		}
		
		return false;
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
