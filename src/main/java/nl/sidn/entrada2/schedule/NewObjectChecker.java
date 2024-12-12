package nl.sidn.entrada2.schedule;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.S3Service;
import nl.sidn.entrada2.service.WorkService;
import software.amazon.awssdk.services.s3.model.S3Object;

@Slf4j
@ConditionalOnExpression(
	    "T(org.apache.commons.lang3.StringUtils).isNotEmpty('${entrada.schedule.new-object-min}')"
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
	private WorkService workService;
	
	@Scheduled(initialDelay = 5000, fixedDelayString = "#{${entrada.schedule.new-object-min:1}*60*1000}")
	public void execute() {
		// find new objects
		log.debug("Start checking for new objects");
		
		try {
			scanForNewObjects();
		} catch (Exception e) {
			log.error("Unexpected exception while scanning for new objects");
		}
		
	}

	public void scanForNewObjects() {
		
		for(S3Object obj : s3Service.ls(bucketName, StringUtils.appendIfMissing(pcapInDir,"/"))) {
			
			log.info("New object found: {}", obj.key());
			
			if(obj.size() == 0) {
				// ignore directories
				return;
			}
			
			workService.process(bucketName, obj.key());
		}		
	}

}
