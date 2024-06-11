package nl.sidn.entrada2.schedule;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.S3Service;
import nl.sidn.entrada2.util.S3ObjectTagName;
import software.amazon.awssdk.services.s3.model.S3Object;

@Slf4j
@Component
public class ScheduledObjectChecker {
	
	@Value("${entrada.object.max-wait-time-secs:3600}")
	private int maxWaitTime;
		
	@Value("${entrada.object.max-proc-time-secs:3600}")
	private int maxProcTime;
	
	@Value("${entrada.s3.bucket}")
	private String bucketName;
	
	@Value("${entrada.s3.pcap-in-dir}")
	private String pcapInDir;
	
	@Value("${entrada.s3.pcap-failed-dir}")
	private String pcapFailedDir;

	@Autowired
	private S3Service s3Service;
	
	@Scheduled(initialDelayString = "#{${entrada.schedule.expired-object:10}*60*1000}", fixedDelayString = "#{${entrada.schedule.expired-object:10}*60*1000}")
	public void execute() {
		 Map<String, String> tags = new HashMap<String, String>();
		
		 LocalDateTime now = LocalDateTime.now();
			
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
						// remove start_ts tag
						//tags.remove(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value);
						//s3Service.tag(bucketName, obj.key(), tags);
						
						log.info("Move failed object {} to fail location", obj.key());
						String file = StringUtils.substringAfterLast(obj.key(), "/");
						s3Service.move(bucketName, obj.key(), pcapFailedDir + "/" + file);
					}
					}
				}else {
					// object has not yet been picked up by a worker, maybe queue was unavailable
					// resend it to the queue to make sure it will be processed
					LocalDateTime objDate = LocalDateTime.ofInstant(obj.lastModified(), ZoneId.systemDefault() );
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
