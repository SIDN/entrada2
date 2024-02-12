package nl.sidn.entrada2.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.context.event.EventListener;
import org.springframework.integration.leader.Context;
import org.springframework.integration.leader.event.OnGrantedEvent;
import org.springframework.integration.leader.event.OnRevokedEvent;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.enrich.geoip.GeoIPService;
import nl.sidn.entrada2.service.enrich.resolver.DnsResolverCheck;
import nl.sidn.entrada2.service.messaging.LeaderQueue;

/**
 * LeaderService enables multiple instances to work together processing pcap files.
 * Only the leader is allowed to make commits to the Iceberg table, this to prevent excessive 
 * commit locking conficts when multiple container try to commit
 */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
@Slf4j
public class LeaderService {
	
	@Autowired
	private GeoIPService geoIPService;
	@Autowired
	private List<DnsResolverCheck> resolverChecks;

	/* leader property is used for non k8s deployments */
	@Value("${entrada.leader:false}")
	private boolean leader;

	@Value("${spring.cloud.kubernetes.leader.role}")
	private String role;

	private Context context;
	
	@Autowired
	private LeaderQueue leaderQueue;

	public boolean isleader() {
		return leader || (this.context != null);
	}


	/**
	 * Handle a notification that this instance has become a leader.
	 * 
	 * @param event on granted event
	 */
	@EventListener
	public void handleEvent(OnGrantedEvent event) {
		log.info("leadership granted: {}", event.getRole());
		this.context = event.getContext();
		
		leaderQueue.start();
		// make sure the reference data is downloaded first time by leader
		// others will wait for data to be present
		log.info("Leader is starting metadata downloads");
		geoIPService.downloadWhenRequired();
		resolverChecks.stream().forEach(c -> c.download());
	}

	/**
	 * Handle a notification that this instance's leadership has been revoked.
	 * 
	 * @param event on revoked event
	 */
	@EventListener
	public void handleEvent(OnRevokedEvent event) {
		log.info("leadership revoked: {}", event.getRole());
		
		this.context = null;
		leaderQueue.stop();
	}

}
