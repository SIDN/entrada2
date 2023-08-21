package nl.sidn.entrada2.schedule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.WorkService;

@Component
@Profile("worker")
@Slf4j
public class ScheduledLivenesChecker {
  
  @Autowired
  private ApplicationContext ctx;
  
  @Autowired
  private WorkService workService;

  
  /**
   * Check if worker is still in good state and able to process pcaps.
   * When LivenessState.BROKEN is set then k8s will restart te container
   * 
   */
  @Scheduled(initialDelayString = "#{${entrada.schedule.liveness:10}*60*1000}", fixedDelayString = "#{${entrada.schedule.liveness:10}*60*1000}")
  public void execute() {
    if(workService.isStalled()) {
      log.error("Worker is stalled, change liveness state to broken");
      AvailabilityChangeEvent.publish(ctx, LivenessState.BROKEN);
    }
  }

}
