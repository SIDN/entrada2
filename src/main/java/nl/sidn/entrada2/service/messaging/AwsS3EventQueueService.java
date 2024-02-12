package nl.sidn.entrada2.service.messaging;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.operations.SendResult;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.RequestMessage;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.util.ConditionalOnAws;
import nl.sidn.entrada2.util.UrlUtil;

@ConditionalOnAws
@Service
@Slf4j
public class AwsS3EventQueueService extends AbstractAwsQueue implements RequestQueue {

	@Value("${entrada.messaging.request.name}-queue")
	private String s3EventQueue;

	@Value("${entrada.messaging.request.name}-queue.fifo")
	private String requestQueue;

	@Autowired
	private SqsTemplate sqsTemplate;

	/**
	 * 
	 * Receive events from S3 bucket and get bucket+key and create new request event
	 */
	@SqsListener(value = "${entrada.messaging.request.name}-queue", id = "${entrada.messaging.request.name}-queue")
	public void receiveMessage(S3EventNotification message) {
		//SqsEventMessage
		log.info("Received SQS message: {}", message);

		if (message.getRecords() != null) {
			
			message	.getRecords().stream().forEach( r -> process(r.getS3().getBucket().getName(),
					UrlUtil.decode(r.getS3().getObject().getKey())));
		}

	}

	/**
	 * Send new RequestMessage to the request fifo queu using a random messagegroupid, this way
	 * any of the entrada instance can receive the request and process it without blocking the
	 * other files in the queue or blocking other queue consumers.
	 */
	private void process(String bucket, String key) {

		sqsTemplate.send(to -> to.queue(requestQueue)
				.payload(new RequestMessage(bucket, key)).messageDeduplicationId(dedupId(bucket, key)));

	}
	
	private String dedupId(String bucket, String key) {
		return StringUtils.deleteWhitespace(bucket + "/" + key);
	}

	@Override
	public String name() {
		return s3EventQueue;
	}

}
