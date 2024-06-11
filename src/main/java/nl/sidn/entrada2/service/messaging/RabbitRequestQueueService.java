package nl.sidn.entrada2.service.messaging;

import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.rabbitmq.client.Channel;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.messaging.S3EventNotification.S3EventNotificationRecord;
import nl.sidn.entrada2.service.WorkService;
import nl.sidn.entrada2.util.ConditionalOnRabbitMQ;
import nl.sidn.entrada2.util.UrlUtil;

@ConditionalOnRabbitMQ
@Service
@Slf4j
public class RabbitRequestQueueService extends AbstractRabbitQueue implements RequestQueue {
	
	@Value("${entrada.messaging.leader.name}")
	private String queueName;

	@Autowired(required = false)
	@Qualifier("rabbitByteTemplate")
	private AmqpTemplate rabbitTemplate;

	@Value("${entrada.messaging.request.name}")
	private String requestQueue;
	@Autowired
	private WorkService workService;
	
	@RabbitListener(id = "${entrada.messaging.request.name}", queues = "${entrada.messaging.request.name}-queue")
	public void receiveMessageManual(S3EventNotification message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {
		log.info("Received s3 event: {}", message);
		
		for (S3EventNotificationRecord rec : message.getRecords()) {
			// check the getEventName, updating the tags may also generate a put event and
			// this should not lead to processing the same file again.
			if (isSupportedEvent(rec.getEventName())) {
				String bucket = rec.getS3().getBucket().getName();
				String key = UrlUtil.decode(rec.getS3().getObject().getKey());

				workService.process(bucket, key);
			}
		}
	}
	
	private boolean isSupportedEvent(String eventName) {
		return StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:Put") || 
				StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:CompleteMultipartUpload") ||
				// some s3 impls also use PutTagging when add deleting a tag
				StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:DeleteTagging") ||
				// puttagging is created when object is resend to queue by updating its tags
				StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:PutTagging");
	}

	@Override
	public String name() {
		return requestQueue;
	}
	
}
