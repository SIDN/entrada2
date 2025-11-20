package nl.sidn.entrada2.schedule;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.S3Service;
import nl.sidn.entrada2.util.S3ObjectTagName;
import software.amazon.awssdk.services.s3.model.S3Object;

@Slf4j
@Component
public class ExpiredObjectChecker {
	
	@Autowired
	private LeaderService leaderService;
	
	@Value("${entrada.object.max-wait-time-secs:3600}")
	private int maxWaitTime;
		
	@Value("${entrada.object.max-proc-time-secs:3600}")
	private int maxProcTime;
	
	@Value("${entrada.s3.bucket}")
	private String bucketName;
	
	@Value("${entrada.s3.pcap-in-dir}")
	private String pcapInDir;
	
	@Value("${entrada.object.max-tries:2}")
	private int maxTries;

	@Autowired
	private S3Service s3Service;
	
	@Scheduled(initialDelayString = "#{${entrada.schedule.expired-object-min:10}*60*1000}", fixedDelayString = "#{${entrada.schedule.expired-object-min:10}*60*1000}")
	public void execute() {
		if (!leaderService.isleader()) {
			// only leader is allowed to continue
			return;
		}
		
		try {
			checkForExperiredObjects();
		} catch (Exception e) {
			log.error("Unexpected exception while checking for expired objects");
		}
	}
	
	public void checkForExperiredObjects() {
		 Map<String, String> tags = new HashMap<String, String>();
		
		 LocalDateTime now = LocalDateTime.now();
		 ZoneId localZone = ZoneId.systemDefault();
			
		// find objects that have ts_start < (now - max_proc_time) and have no ts_end
		for(S3Object obj : s3Service.ls(bucketName, StringUtils.appendIfMissing(pcapInDir,"/"))) {
			
			log.debug("Checking: {}", obj.key());
			
			if(obj.size() == 0) {
				// ignore directories
				return;
			}
			
			
			if(s3Service.tags(bucketName, obj.key(), tags)) {
				// check for ts_start without and ts_end
				if(tags.keySet().contains(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value) ) {
					if(!tags.keySet().contains(S3ObjectTagName.ENTRADA_PROCESS_TS_END.value)) {					
					// check if object claim is expired
					Optional<LocalDateTime> startDate = stringToDate(tags.get(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value));
						if(startDate.isEmpty() || startDate.get().plusSeconds(maxProcTime).isBefore(now)) {
							if(tags.keySet().contains(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value)) {
								String value = tags.get(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value);
								if(NumberUtils.isCreatable(value)) {
									int tries = NumberUtils.createInteger(tags.get(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value));
									if(tries < maxTries){
										// expired object and max tries not yet reached, remove  start tag
										// this will cause the object to be processed again
										tags.remove(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value);
										tags.put(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value, tries++ + "");
										tags.put(S3ObjectTagName.ENTRADA_NOT_COMPLETED.value, "true");
										s3Service.tag(bucketName, obj.key(), tags);
										
										log.info("Object not processed correctly, doing retry for: {}", obj.key());
									}
								}
							}
						}
					}
				}else {
					// object has not yet been picked up by a worker, maybe queue was unavailable
					// resend it to the queue to make sure it will be processed
					LocalDateTime objDate = LocalDateTime.ofInstant(obj.lastModified(), localZone );
					if(objDate.plusSeconds(maxWaitTime).isBefore(now)) {
						// setting the tags to the object again, will cause an s3:ObjectCreated:PutTagging event to be sent to the queue
						log.info("Object {} was not picked up, resending it to queue for processing",obj.key());
						tags.put(S3ObjectTagName.ENTRADA_WAIT_EXPIRED.value, "true");
						s3Service.tag(bucketName,  obj.key(), tags);
					}
				}				
			}	
			tags.clear();
		}
	}

	
	private Optional<LocalDateTime> stringToDate(String datetr) {
		try {
			return Optional.of(LocalDateTime.parse(datetr, DateTimeFormatter.ISO_DATE_TIME));
		} catch (Exception e) {
			// ignore error
		}
		return Optional.empty();
	}

}
