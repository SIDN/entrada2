package nl.sidn.entrada2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.influxdb.client.InfluxDBClient;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.LeaderQueueService;
import nl.sidn.entrada2.service.LeaderService;

@Component
@Slf4j
public class StartupListener {
	
	@Autowired
	private LeaderService leaderService;
	@Autowired
	private LeaderQueueService leaderQueueService;
	@Autowired
	private InfluxDBClient influxClient;

	@EventListener
	public void onApplicationEvent(ContextRefreshedEvent event) {

		if(leaderService.isleader()) {
			log.info("This is the leader, start listening to leader queue");
			leaderQueueService.start();
		}
	}
	
	@EventListener
	public void onApplicationEvent(ContextClosedEvent event) {

		influxClient.close();
	}

}
