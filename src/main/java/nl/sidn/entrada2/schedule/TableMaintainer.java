package nl.sidn.entrada2.schedule;

import org.apache.iceberg.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TableMaintainer {

	@Value("${iceberg.table.snapshot-max:1}")
	private int maxSnapshots;
	@Autowired
	private Table table; 
	
	
	@Scheduled(cron = "${entrada.schedule.table-maintainer-cron}")
	public void execute() {	
		
		log.info("Expire snapshots, keeping last {} snapshots", maxSnapshots);
		
		try {
			table.expireSnapshots()
				.retainLast(maxSnapshots)
				.commit();
		} catch (Exception e) {
			log.error("Unexpected exception while expring snapshots");
		}
	}



}
