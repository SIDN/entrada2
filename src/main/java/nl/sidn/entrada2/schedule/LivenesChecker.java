package nl.sidn.entrada2.schedule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.WorkService;

@Component
@Slf4j
public class LivenesChecker {
  
  @Autowired
  private ApplicationContext ctx;
  @Autowired
  private ApplicationAvailability applicationAvailability;
  
  @Autowired
  private WorkService workService;


/**
   * Check if worker is still in good state and able to process pcaps.
   * When LivenessState.BROKEN is set then k8s will restart te container
   * 
   */
  @Scheduled(initialDelayString = "#{${entrada.schedule.liveness-min:5}*60*1000}", fixedDelayString = "#{${entrada.schedule.liveness-min:5}*60*1000}")
  public void execute() {
    if(workService.isStalled()) {
      log.error("Application is stalled, change liveness state to broken");
      AvailabilityChangeEvent.publish(ctx, LivenessState.BROKEN);
    }else {
      // not stalled, flip if needed
      if(applicationAvailability.getLivenessState() ==  LivenessState.BROKEN) {
        AvailabilityChangeEvent.publish(ctx, LivenessState.CORRECT);
      }
    }
  }

}
