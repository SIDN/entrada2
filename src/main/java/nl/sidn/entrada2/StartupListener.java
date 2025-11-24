package nl.sidn.entrada2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.influxdb.v3.client.InfluxDBClient;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.service.messaging.LeaderQueue;

@Component
@Slf4j
public class StartupListener {

	@Value("${entrada.s3.bucket}")
	private String bucketName;

	@Autowired
	private LeaderService leaderService;
	@Autowired
	private LeaderQueue leaderQueue;
	@Autowired(required = false)
	private InfluxDBClient influxClient;

	@EventListener
	public void onApplicationEvent(ContextRefreshedEvent event) {

		if (leaderService.isleader()) {
			log.info("This is the leader, start listening to leader queue");
			leaderQueue.start();
		}else {
			log.info("This is NOT the leader, make sure not to be listening to leader queue");
			// not the leader, make sure it is not listing to leader queue
			leaderQueue.stop();
		}
	}

	@EventListener
	public void onApplicationEvent(ContextClosedEvent event) {
		if (influxClient != null) {
			try {
				influxClient.close();
			} catch (Exception e) {
				log.error("Error closing InfluxDB client");
			}
		}
	}

	
	@EventListener
    public void onEvent(AvailabilityChangeEvent<LivenessState> event) {
        switch (event.getState()) {
        case BROKEN:
        	log.error("This pod is broken");
            break;
        case CORRECT:
        	log.info("This pod is working correct");
        }
    }
}
