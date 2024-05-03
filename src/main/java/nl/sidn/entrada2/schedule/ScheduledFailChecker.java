package nl.sidn.entrada2.schedule;

import java.time.LocalDateTime;
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
public class ScheduledFailChecker {
	
	@Value("#{${entrada.object-max-proc-time:30}*60}")
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
		 
		// find objects that have ts_start < (now - max_proc_time) and have no ts_end
		for(S3Object obj : s3Service.ls(bucketName, pcapInDir)) {
			
			log.debug("Checking: {}", obj.key());
			
			
			if(s3Service.tags(bucketName, obj.key(), tags)) {
				// check for ts_start without and ts_end
				if(tags.keySet().contains(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value) &&
						!tags.keySet().contains(S3ObjectTagName.ENTRADA_PROCESS_TS_END.value)) {
										
					// check if object claim is expired
					Optional<LocalDateTime> startDate = stringToDate(tags.get(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value));
					
					if(startDate.isEmpty() || startDate.get().plusSeconds(maxProcTime).isBefore(LocalDateTime.now())) {
						// remove start_ts tag
						//tags.remove(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value);
						//s3Service.tag(bucketName, obj.key(), tags);
						
						log.info("Move failed object {} to fail location", obj.key());
						String file = StringUtils.substringAfterLast(obj.key(), "/");
						s3Service.move(bucketName, obj.key(), pcapFailedDir + "/" + file);
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
