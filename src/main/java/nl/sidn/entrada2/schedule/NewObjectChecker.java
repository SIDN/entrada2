package nl.sidn.entrada2.schedule;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
import software.amazon.awssdk.services.s3.model.S3Object;

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
		
		int counter = 0;
		int counterAdded = 0;
		List<S3Object> s3Objects = s3Service.ls(bucketName, StringUtils.appendIfMissing(pcapInDir,"/"));
		
		s3Objects.sort(Comparator.comparing(S3Object::key));
		Map<String, String> tags = new HashMap<String, String>();
		
		for( S3Object s3Object: s3Objects) {
			
			 if(s3Service.tags(bucketName, s3Object.key(), tags)){
				 if(StringUtils.isEmpty(tags.get(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value))) {
					 
					if(log.isDebugEnabled()) {
						log.debug("New object found: {}/{}", bucketName, s3Object.key());
					}
					
					tags.put(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value, "true");
					if(s3Service.tag(bucketName, s3Object.key(), tags)) {
						rabbitTemplate.convertAndSend(requestQueue + "-exchange", requestQueue, createEvent(bucketName, s3Object.key()));
						counterAdded++;
					}else {
						// problem setting tags
						log.error("Failed to set tags on newly detected object: ", s3Object.key());
					}
					
					counter++;
					 
				 }
			 }
			 
			 tags.clear();
		}
		
		log.info("Detected {} new object(s) and added {} object(s) to request queue", counter, counterAdded);
	}
	
	private S3EventNotification createEvent(String bucket, String key) {
		 new S3EventNotification.S3BucketEntity(bucket, null, null);
			S3Entity e = new S3Entity(null, new S3EventNotification.S3BucketEntity(bucketName, null, null),
					new S3EventNotification.S3ObjectEntity(key, null, null, null, null), null);
			
			List<S3EventNotificationRecord> records = List.of(new S3EventNotificationRecord(null, "s3:ObjectCreated:Put", null, null, null, null, null, e, null ,null,null, null, null));
			
			return new S3EventNotification(records);

	}

}
