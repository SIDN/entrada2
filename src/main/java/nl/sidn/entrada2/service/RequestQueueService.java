package nl.sidn.entrada2.service;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.rabbitmq.client.Channel;

import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.messaging.S3EventNotification.S3EventNotificationRecord;

@Service
@Slf4j
public class RequestQueueService extends AbstractQueueService {
	

	@Value("${entrada.messaging.request-queue}")
	private String requestQueue;
	@Autowired
	private WorkService workService;
	
	private URLCodec urlCodec = new URLCodec();

	
	@ConditionalOnProperty(prefix = "spring.rabbitmq", name = "enabled", havingValue = "true")
	@RabbitListener(id = "${entrada.messaging.request-queue}", queues = "${entrada.messaging.request-queue}-queue")
	public void receiveMessageManual(S3EventNotification message, Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long tag) {
		
		handleMessage(message);
		// channel.basicAck(tag, false);

	}

	@ConditionalOnProperty(prefix = "spring.cloud.aws.sqs", name = "enabled", havingValue = "true")
	@SqsListener("entrada-s3-events.fifo")
	public void receiveMessage(S3EventNotification message, @Header("SenderId") String senderId) {
		
		handleMessage(message);
		
	}
	
	private String urlDecode(String url) {
		try {
			return urlCodec.decode(url);
		} catch (DecoderException e) {
			log.error("Error decoding url: {}", url);
		}
		
		return "";
	}
	
	private void handleMessage(S3EventNotification message) {
		
		log.info("Received RabbitMQ message: {}", message);

		for (S3EventNotificationRecord rec : message.getRecords()) {
			// check the getEventName, updating the tags may also generate a put event and
			// this should not lead to processing the same file again.
			if (isNewFile(rec.getEventName())) {
				String bucket = rec.getS3().getBucket().getName();
				String key = urlDecode(rec.getS3().getObject().getKey());

				workService.process(bucket, key);
			}

		}

		
	}
	
	private boolean isNewFile(String eventName) {
		return StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:Put") || 
				StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:CompleteMultipartUpload");
	}

	@Override
	public String name() {
		return requestQueue;
	}
	


}
