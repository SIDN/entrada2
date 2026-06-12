package nl.sidn.entrada2.config;

import java.net.URI;
import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
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

	private final EntradaS3Properties s3Properties;

	public S3Config(EntradaS3Properties s3Properties) {
		this.s3Properties = s3Properties;
	}

	private boolean isRunningOnAws() {
		return StringUtils.isBlank(s3Properties.getEndpoint());
	}
	

	/**
	 * Normal and default s3 client for reading larger pcap files, has longer timeouts
	 * @return
	 */
	@Bean
	@Primary
	public S3Client s3() {

		if (isRunningOnAws()) {
			return S3Client.builder().forcePathStyle(s3Properties.getPathStyleAccess()).build();
		}
		// when not running on aws, make sure the s3 endpoint is configured
		 S3ClientBuilder builder = S3Client.builder()
				 .forcePathStyle(s3Properties.getPathStyleAccess())
				 .region(Region.of(s3Properties.getRegion()))
				 .httpClientBuilder(ApacheHttpClient.builder()
						 	.connectionTimeout(Duration.ofSeconds(10))
			                .socketTimeout(Duration.ofSeconds(60))  // must be high enough to avoid false timeouts during bursty streaming
			                .maxConnections(20))
				 .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(s3Properties.getAccessKey(), s3Properties.getSecretKey())))
				 .overrideConfiguration(ClientOverrideConfiguration.builder()
						 	.retryStrategy(StandardRetryStrategy.builder()
						        .maxAttempts(2)
								.backoffStrategy(BackoffStrategy.fixedDelay(Duration.ofSeconds(2)))
								.build())
						 	// apiCallAttemptTimeout only covers TCP connect + HTTP request/headers phase.
						 	// once getObject() returns a ResponseInputStream, only socketTimeout governs stalled reads.
						 	.apiCallAttemptTimeout(Duration.ofSeconds(30))
					        .apiCallTimeout(Duration.ofMinutes(10))  // files process in a few minutes; 10 min gives headroom for a retry
				        .build());

		 applyEndpointOverride(builder);
		 return builder.build();
	}
	
	/**
	 * Only use fastClient for fast operation such as getting and setting tags
	 * has shorter timeouts
	 * @return
	 */
	@Bean
	public S3Client fastClient() {
			
		 S3ClientBuilder builder = S3Client.builder()
				 .forcePathStyle(s3Properties.getPathStyleAccess())
				 .region(Region.of(s3Properties.getRegion()))
				 .httpClientBuilder(ApacheHttpClient.builder()
						 	.connectionTimeout(Duration.ofSeconds(5))
			                .socketTimeout(Duration.ofSeconds(10))
		                .maxConnections(20))
				 .credentialsProvider(StaticCredentialsProvider.create(
					AwsBasicCredentials.create(s3Properties.getAccessKey(),
												s3Properties.getSecretKey())))
				 .overrideConfiguration(ClientOverrideConfiguration.builder()
						 	.retryStrategy(StandardRetryStrategy.builder()
						        .maxAttempts(2)
								.backoffStrategy(BackoffStrategy.fixedDelay(Duration.ofSeconds(1)))
								.build())
							.apiCallAttemptTimeout(Duration.ofSeconds(3))  // per attempt
					        .apiCallTimeout(Duration.ofSeconds(8)) // total time for all attempts. getting tags should be fast, so set it low to fail fast when there are issues with the s3 connection
			        .build());

		 applyEndpointOverride(builder);
		 return builder.build();
	}

	private void applyEndpointOverride(S3ClientBuilder builder) {
		if (StringUtils.isNotBlank(s3Properties.getEndpoint())) {
			builder.endpointOverride(URI.create(s3Properties.getEndpoint()));
		}
	}

	@PostConstruct
	private void init() {
		log.info("Using input s3 endpoint: {}", s3Properties.getEndpoint());
		log.info("Using input s3 bucket: {}", s3Properties.getBucket());
		
		if(!provisioningEnabled) {
			log.info("Provisioning is disabled, do not create required bucket");
			return;
		}

		if (!isBucketExist(s3(), s3Properties.getBucket())) {
			createBucket(s3(), s3Properties.getBucket(), isRunningOnAws());
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
