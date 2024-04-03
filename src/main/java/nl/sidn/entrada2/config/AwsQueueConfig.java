package nl.sidn.entrada2.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.ListenerMode;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.util.ConditionalOnAws;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamPolicyWriter;
import software.amazon.awssdk.policybuilder.iam.IamPrincipalType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.FilterRule;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.NotificationConfigurationFilter;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.QueueConfiguration;
import software.amazon.awssdk.services.s3.model.S3KeyFilter;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

@ConditionalOnAws
@Configuration
@Slf4j
public class AwsQueueConfig {
	
	@Value("${entrada.provisioning.enabled:true}")
	private boolean provisioningEnabled;

	@Value("${entrada.messaging.request.name}")
	private String requestQueue;
	@Value("${entrada.messaging.request.retention}")
	private int requestQueueRetention;
	@Value("${entrada.messaging.request.visibility-timeout}")
	private int requestQueueVisibilty;

	@Value("${entrada.messaging.leader.name}")
	private String leaderQueue;
	@Value("${entrada.messaging.leader.retention}")
	private int leaderQueueRetention;
	@Value("${entrada.messaging.leader.visibility-timeout}")
	private int leaderQueueVisibilty;

	@Value("${entrada.messaging.command.name}")
	private String commandQueue;
	@Value("${entrada.messaging.command.retention}")
	private int commandQueueRetention;
	@Value("${entrada.messaging.command.visibility-timeout}")
	private int commandQueueVisibilty;

	@Value("${entrada.s3.bucket}")
	private String bucketName;

	@Value("${entrada.s3.pcap-directory}")
	private String pcapDirectory;

	@Autowired
	private SqsClient sqsClient;

	@Autowired
	private S3Client s3Client;

	@Bean
	public SqsClient sqsClient() {
		return SqsClient.builder().build();
	}

	@PostConstruct
	public void initialize() {
		
		if(!provisioningEnabled) {
			log.info("Provisioning is disabled, do not create required queues");
			return;
		}

		String queueName = commandQueue + "-queue";
		if (!isQueueExist(queueName)) {
			createSQSQueue(queueName, false, commandQueueRetention, commandQueueVisibilty);
		}

		// first create std queue to send all object create event from bucket to
		queueName = requestQueue + "-queue";
		if (!isQueueExist(queueName)) {
			String queueUrl = createSQSQueue(queueName, false, requestQueueRetention, requestQueueVisibilty);
			// create permission for s3 bucket to send events to queue

			Optional<String> optQueueArn = lookupRequestQueueArn(queueUrl);
			if (!optQueueArn.isEmpty()) {
				attachQueuPolicy(queueUrl, createQueuePolicy(optQueueArn.get(), "arn:aws:s3:::" + bucketName));
			} else {
				log.error("ARN for queue url not found, cannot attach policy, url: {}", queueUrl);
			}

			// create event for s3 bucket
			createBucketEventNotificationFor(optQueueArn.get());
		}

		// create fifo queue where entrada will send new file event to
		queueName = requestQueue + "-queue.fifo";
		if (!isQueueExist(queueName)) {
			createSQSQueue(queueName, true, requestQueueRetention, requestQueueVisibilty);
		}

		// create queue for leader (iceberg) events
		queueName = leaderQueue + "-queue.fifo";
		if (!isQueueExist(queueName)) {
			createSQSQueue(queueName, true, leaderQueueRetention, leaderQueueVisibilty);
		}
	}

	private boolean isQueueExist(String queueName) {

		try {
			GetQueueUrlRequest urlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
			GetQueueUrlResponse r = sqsClient.getQueueUrl(urlRequest);
			return r.sdkHttpResponse().isSuccessful();
		} catch (QueueDoesNotExistException e) {
			// do nothing
		}

		return false;
	}

	private Optional<String> lookupRequestQueueArn(String queueUrl) {

		try {
			GetQueueAttributesRequest request = GetQueueAttributesRequest.builder().queueUrl(queueUrl)
					.attributeNames(QueueAttributeName.QUEUE_ARN).build();
			GetQueueAttributesResponse response = sqsClient.getQueueAttributes(request);
			return Optional.ofNullable(response.attributes().get(QueueAttributeName.QUEUE_ARN));
		} catch (QueueDoesNotExistException e) {
			// do nothing
		}

		return Optional.empty();
	}

	private String createSQSQueue(String queueName, boolean fifo, int retention, int visibility) {

		// create new quue
		Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
		if (fifo) {
			queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
			queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true");
			// enabling fifo high throughput queue
			// see:
			// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html
			queueAttributes.put(QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup");
			queueAttributes.put(QueueAttributeName.FIFO_THROUGHPUT_LIMIT, "perMessageGroupId");
		}

		queueAttributes.put(QueueAttributeName.SQS_MANAGED_SSE_ENABLED, "false");
		queueAttributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibility));
		queueAttributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, String.valueOf(10));
		queueAttributes.put(QueueAttributeName.MESSAGE_RETENTION_PERIOD, String.valueOf(retention));

		CreateQueueRequest createFifoQueueRequest = CreateQueueRequest.builder().queueName(queueName)
				.attributes(queueAttributes).build();

		String requestQueueUrl = sqsClient.createQueue(createFifoQueueRequest).queueUrl();
		log.info("Created new SQS queue, URL: {}", requestQueueUrl);
		return requestQueueUrl;
	}

	public void createBucketEventNotificationFor(String queueArn) {

		S3KeyFilter fltr = S3KeyFilter.builder()
				.filterRules(FilterRule.builder().name("prefix").value(pcapDirectory + "/").build()).build();

		NotificationConfigurationFilter filterConf = NotificationConfigurationFilter.builder().key(fltr).build();

		QueueConfiguration destQueue = QueueConfiguration.builder().queueArn(queueArn).events(Event.S3_OBJECT_CREATED)
				.filter(filterConf).id("entrada-new-object").build();

		NotificationConfiguration not = NotificationConfiguration.builder().queueConfigurations(destQueue).build();

		PutBucketNotificationConfigurationRequest notificationConfiguration = PutBucketNotificationConfigurationRequest
				.builder().bucket(bucketName).notificationConfiguration(not).build();

		s3Client.putBucketNotificationConfiguration(notificationConfiguration);

	}

	private void attachQueuPolicy(String queueUrl, String policy) {

		Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
		queueAttributes.put(QueueAttributeName.POLICY, policy);

		SetQueueAttributesRequest updateQueueRequest = SetQueueAttributesRequest.builder().queueUrl(queueUrl)
				.attributes(queueAttributes).build();

		sqsClient.setQueueAttributes(updateQueueRequest);
	}

	private String createQueuePolicy(String targetArn, String sourceArn) {
		IamPolicy policy = IamPolicy.builder()
				.addStatement(b -> b.effect(IamEffect.ALLOW).addAction("sqs:SendMessage").addResource(targetArn)
						.addPrincipal(IamPrincipalType.SERVICE, "s3.amazonaws.com")
						.addConditions(IamConditionOperator.ARN_EQUALS, "aws:SourceArn", List.of(sourceArn)))
				.build();

		return policy.toJson(IamPolicyWriter.builder().prettyPrint(true).build());
	}

	/**
	 * Create factory for command queue, make sure the listener only reads 1 message
	 * per poll and does manual ack (which we never send, so the message is only
	 * delete from the queue when it expires)
	 */
	@Bean
	SqsMessageListenerContainerFactory<Object> commandSqsListenerContainerFactory(SqsAsyncClient sqsAsyncClient) {
		return SqsMessageListenerContainerFactory.builder().sqsAsyncClient(sqsAsyncClient)
				.configure(options -> options
						.listenerMode(ListenerMode.SINGLE_MESSAGE)
						.maxConcurrentMessages(1)
						.maxMessagesPerPoll(1)
						.acknowledgementMode(AcknowledgementMode.MANUAL)
						.pollTimeout(Duration.ofSeconds(10)))
				.build();
	}


	@Bean
	SqsMessageListenerContainerFactory<Object> leaderSqsListenerContainerFactory(SqsAsyncClient sqsAsyncClient) {
		return SqsMessageListenerContainerFactory.builder().sqsAsyncClient(sqsAsyncClient)
				.configure(options -> options
						.listenerMode(ListenerMode.SINGLE_MESSAGE)
						.acknowledgementMode(AcknowledgementMode.ON_SUCCESS)
						.maxConcurrentMessages(1)
						.maxMessagesPerPoll(1)
						.pollTimeout(Duration.ofSeconds(10))
						)
				.build();
	}

	/**
	 * Create factory for the other queues, make sure only read 1 message per poll.
	 * and use random messagegroupid per message so multiple subscribers can read
	 * parallel from the queues.
	 */
	@Bean
	@Primary
	SqsMessageListenerContainerFactory<Object> defaultSqsListenerContainerFactory(SqsAsyncClient sqsAsyncClient) {
		return SqsMessageListenerContainerFactory.builder().sqsAsyncClient(sqsAsyncClient)
				.configure(options -> options
						.acknowledgementMode(AcknowledgementMode.ON_SUCCESS)
						.maxConcurrentMessages(1)
						.maxMessagesPerPoll(1)
						.pollTimeout(Duration.ofSeconds(10)))
				.build();
	}

}
