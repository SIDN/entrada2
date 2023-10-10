package nl.sidn.entrada2.schedule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.WorkQueueService;

@Slf4j
@Component
@Profile("controller")
public class ScheduledInputMonitor {

  @Autowired
  private WorkQueueService workQueueService;

  /**
   * Check if files marked as served are processed if not then clear the served attribute
   * which will allow for the file to be processed again.
   */
  @Scheduled(initialDelayString = "#{${entrada.schedule.input-monitor:10}*60*1000}", fixedDelayString = "#{${entrada.schedule.input-monitor:10}*60*1000}")
  public void execute() {
    long expired = workQueueService.resetExpired();
    if(expired > 0) {
      log.info("Updated {} expired input file(s)", expired);
    }
  }

}
