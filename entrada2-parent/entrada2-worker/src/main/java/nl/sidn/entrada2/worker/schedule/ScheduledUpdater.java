package nl.sidn.entrada2.worker.schedule;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import nl.sidn.entrada2.worker.service.enrich.geoip.GeoIPService;
import nl.sidn.entrada2.worker.service.enrich.resolver.DnsResolverCheck;

@Component
public class ScheduledUpdater {

  @Autowired
  private GeoIPService geoIPService;
  
  @Autowired
  private List<DnsResolverCheck> resolverChecks;

  /**
   * Check if reference data needs to be updated.
   * No need to do this during startup
   */
  @Scheduled(initialDelayString = "#{${entrada.schedule.geoip}*60*60*1000}", fixedDelayString = "#{${entrada.schedule.geoip}*60*60*1000}")
  public void execute() {
    geoIPService.load();
    resolverChecks.stream().forEach( c -> c.load());
  }

}
