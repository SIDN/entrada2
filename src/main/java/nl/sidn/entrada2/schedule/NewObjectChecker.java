package nl.sidn.entrada2.schedule;

import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.messaging.S3EventNotification.S3Entity;
import nl.sidn.entrada2.messaging.S3EventNotification.S3EventNotificationRecord;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.S3Service;
import nl.sidn.entrada2.util.S3ObjectTagName;

@Slf4j
@ConditionalOnExpression(
	    "T(org.apache.commons.lang3.StringUtils).isNotEmpty('${entrada.schedule.new-object-secs}')"
	)
@Component
public class NewObjectChecker {
	
	@Value("${entrada.s3.bucket}")
	private String bucketName;
	
	@Value("${entrada.s3.pcap-in-dir}")
	private String pcapInDir;

	@Autowired
	private S3Service s3Service;
	
	@Autowired
	private LeaderService leaderService;
	
	@Value("${entrada.messaging.request.name}")
	private String requestQueue;
	
	@Autowired(required = false)
	@Qualifier("rabbitJsonTemplate")
	private AmqpTemplate rabbitTemplate;
	
	
	@Scheduled(initialDelay = 5000, fixedDelayString = "#{${entrada.schedule.new-object-secs:30}*1000}")
	public void execute() {
		if (!leaderService.isleader()) {
			// only leader is allowed to continue
			return;
		}
		// find new objects
		log.debug("Start checking for new objects");
		
		try {
			scanForNewObjects();
		} catch (Exception e) {
			log.error("Unexpected exception while scanning for new objects");
		}
		
	}

	public void scanForNewObjects() {
		
		s3Service.ls(bucketName, StringUtils.appendIfMissing(pcapInDir,"/")).stream()
		.filter( obj -> obj.size() > 0)
		// get tags for object, may not be very efficient when many new objects are detected
		.map(obj -> Pair.of(obj.key(), s3Service.tags(bucketName, obj.key())))
		.sorted(Comparator.comparing(obj -> StringUtils.defaultString(obj.getValue().get(S3ObjectTagName.ENTRADA_OBJECT_TS.value))))
		.map(Pair::getKey)
		.forEach( obj -> {
			log.info("New object found: {}/{}", bucketName, obj);
			
			//  send new object to the request queue so the object can be processed by any listening entrada instance
			rabbitTemplate.convertAndSend(requestQueue + "-exchange", requestQueue, createEvent(bucketName, obj));
		});
		
	}
	
	private S3EventNotification createEvent(String bucket, String key) {
		 new S3EventNotification.S3BucketEntity(bucket, null, null);
			S3Entity e = new S3Entity(null, new S3EventNotification.S3BucketEntity(bucketName, null, null),
					new S3EventNotification.S3ObjectEntity(key, null, null, null, null), null);
			
			List<S3EventNotificationRecord> records = List.of(new S3EventNotificationRecord(null, "s3:ObjectCreated:Put", null, null, null, null, null, e, null ,null,null, null, null));
			
			return new S3EventNotification(records);

	}

}
