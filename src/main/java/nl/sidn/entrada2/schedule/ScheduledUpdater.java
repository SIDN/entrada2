package nl.sidn.entrada2.schedule;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.enrich.geoip.GeoIPService;
import nl.sidn.entrada2.service.enrich.resolver.DnsResolverCheck;

@Slf4j
@Component
public class ScheduledUpdater {

	@Autowired
	private LeaderService leaderService;
	@Autowired
	private GeoIPService geoIPService;

	@Autowired
	private List<DnsResolverCheck> resolverChecks;

	/**
	 * Check if reference data needs to be updated. No need to do this during
	 * startup
	 */
	@Scheduled(initialDelayString = "#{${entrada.schedule.updater:60}*60*1000}", fixedDelayString = "#{${entrada.schedule.updater:60}*60*1000}")
	public void execute() {
		if (leaderService.isleader()) {
			// load new ref data from source to shared s3 location
			geoIPService.downloadWhenRequired();
			resolverChecks.stream().forEach(c -> c.download());
		}
		
		// load data from from shared s3 location
		log.info("Start updating reference data");
		geoIPService.update();
		resolverChecks.stream().forEach(c -> c.update());
	}

}
