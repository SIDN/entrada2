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
import nl.sidn.entrada2.config.EntradaS3Properties;
import nl.sidn.entrada2.messaging.S3EventNotification;
import nl.sidn.entrada2.messaging.S3EventNotification.S3Entity;
import nl.sidn.entrada2.messaging.S3EventNotification.S3EventNotificationRecord;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.S3Service;
import nl.sidn.entrada2.util.S3ObjectTagName;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * This class is responsible for checking for new objects in the configured s3 bucket and prefixes, it does this by listing all objects and checking if they have a specific tag, if not it adds the tag and sends a message to the request queue
 * The tag is used to prevent multiple instances of Entrada from processing the same object when running in a clustered environment
 * The class is only active when the property "entrada.schedule.new-object-secs" is set to a value greater than 0
 * 
 * This is only used as a fallback for when S3 event notifications are not available, it is recommended to use S3 event notifications instead of this class for better performance and reliability
 * 
 */
@Slf4j
@ConditionalOnExpression(
	    "T(org.apache.commons.lang3.StringUtils).isNotEmpty('${entrada.schedule.new-object-secs}')"
	)
@Component
public class NewObjectChecker {

	@Autowired
	private EntradaS3Properties s3Properties;

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
		log.info("Start checking for new objects");
		
		try {
			for(String prefix: s3Properties.getPcapInPrefixes()) {
				log.info("Start checking for new objects with prefix: {}", prefix);
				int counterAdded = scanForNewObjects(prefix);
				log.info("Detected {} new object(s) for prefix: {}", counterAdded, prefix);
			}
		} catch (Exception e) {
			log.error("Unexpected exception while scanning for new objects");
		}
	}

	public int scanForNewObjects(String prefix) {
		
		int counterAdded = 0;
		List<S3Object> s3Objects = s3Service.ls(s3Properties.getBucket(), StringUtils.appendIfMissing(prefix,"/"));

		s3Objects.sort(Comparator.comparing(S3Object::key));
		Map<String, String> tags = new HashMap<String, String>();
		
		for( S3Object s3Object: s3Objects) {

			 if(s3Service.tags(s3Properties.getBucket(), s3Object.key(), tags)){
				 if(StringUtils.isEmpty(tags.get(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value))) {
					 
					if(log.isDebugEnabled()) {
						log.debug("New object found: {}/{}", s3Properties.getBucket(), s3Object.key());
					}
					
					tags.put(S3ObjectTagName.ENTRADA_OBJECT_DETECTED.value, "true");
					if(s3Service.tag(s3Properties.getBucket(), s3Object.key(), tags)) {
						rabbitTemplate.convertAndSend(requestQueue + "-exchange", requestQueue, createEvent(s3Properties.getBucket(), s3Object.key()));
						counterAdded++;
					}else {
						// problem setting tags
						log.error("Failed to set tags on newly detected object: ", s3Object.key());
					}
				 }
			 }
			 tags.clear();
		}
		return counterAdded;
	}
	
	private S3EventNotification createEvent(String bucket, String key) {
		 new S3EventNotification.S3BucketEntity(bucket, null, null);
			S3Entity e = new S3Entity(null, new S3EventNotification.S3BucketEntity(s3Properties.getBucket(), null, null),
					new S3EventNotification.S3ObjectEntity(key, null, null, null, null), null);
			
			List<S3EventNotificationRecord> records = List.of(new S3EventNotificationRecord(null, "s3:ObjectCreated:Put", null, null, null, null, null, e, null ,null,null, null, null));
			
			return new S3EventNotification(records);

	}

}
