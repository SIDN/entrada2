package nl.sidn.entrada2.schedule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.IcebergService;

@Component
@Slf4j
public class ScheduledIcebergCommitter {

  @Autowired
  private IcebergService icebergService;


  @Scheduled(initialDelayString = "#{${entrada.schedule.iceberg-committer:69}*1000}",
      fixedDelayString = "#{${entrada.schedule.iceberg-committer:60}*1000}")
  public void execute() {
    try {
      if (icebergService.isDataFileToCommit()) {
        log.info("New datafiles received, start commit");
        icebergService.commit();
      }
    } catch (Exception e) {
      log.error("Error while committing datafiles", e);
    }
  }

}
