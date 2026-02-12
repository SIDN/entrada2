package nl.sidn.entrada2.service.messaging;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
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

	@Value("${entrada.messaging.request.name}")
	private String requestQueue;
	@Autowired
	private WorkService workService;
	
	@RabbitListener(id = "${entrada.messaging.request.name}", queues = "${entrada.messaging.request.name}-queue")
	public void receiveMessageManual(S3EventNotification message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
		
		String bucket = null;
		String key = null;
		boolean result = false;
		
		//always single record?
		for (S3EventNotificationRecord rec : message.getRecords()) {
			
			// check the getEventName, updating the tags may also generate a put event and
			// this should not lead to processing the same file again.
			bucket = rec.getS3().getBucket().getName();
			key = UrlUtil.decode(rec.getS3().getObject().getKey());
			
			if (isSupportedEvent(rec.getEventName())) {
				log.info("Received s3 event for: {}/{}", bucket, key);
				try {
					result = workService.process(bucket, key);
				} catch (Exception e) {
					log.error("Unhandled exception", e);
					result = false;
					break;
				}
			}else {
				log.error("Unsupported s3 event for: {}/{}", bucket, key);
				result = true;
			}
		}
		
		done(channel, tag, result);
	}
	
	private void done(Channel channel, long tag, boolean ok) {
		if(ok) {
			try {
				channel.basicAck(tag, false);
			} catch (IOException e) {
				log.error("Error sending ack for tag: {}", tag);
			}
		}else {
			try {
				channel.basicNack(tag, false, false);
			} catch (IOException e) {
				log.error("Error sending ack for tag: {}", tag);
			}
		}
	}
	
	private boolean isSupportedEvent(String eventName) {
		return StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:Put") || 
				StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:CompleteMultipartUpload") ||
				// some s3 implementations also use Put-Tagging when deleting a tag
				StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:DeleteTagging") ||
				// put-tagging is created when object is re-send to queue by updating its tags
				StringUtils.equalsIgnoreCase(eventName, "s3:ObjectCreated:PutTagging");
	}

	@Override
	public String name() {
		return requestQueue;
	}
	
}
