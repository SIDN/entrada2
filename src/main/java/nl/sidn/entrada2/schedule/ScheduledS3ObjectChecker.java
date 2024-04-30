package nl.sidn.entrada2.schedule;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.enrich.geoip.GeoIPService;
import nl.sidn.entrada2.service.enrich.resolver.DnsResolverCheck;

@Slf4j
@Component
public class ScheduledS3ObjectChecker {
	
	@Value("${entrada.object-max-proc-time:30}")
	private int maxProcTime;


	@Scheduled(initialDelayString = "#{${entrada.schedule.expired-object:10}*60*1000}", fixedDelayString = "#{${entrada.schedule.expired-object:10}*60*1000}")
	public void execute() {

		// find objects that have ts_start < (now - max_proc_time) and have no ts_end
		
		// release object by removing ts_start tag
		
		
	}

}
