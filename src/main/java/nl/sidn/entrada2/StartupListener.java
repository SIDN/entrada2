package nl.sidn.entrada2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.WorkService;

@Component
@Slf4j
@Profile("worker")
public class StartupListener {

  @Autowired
  private WorkService workService;

  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent event) {

    log.info("Start worker processing thread");
    workService.run();
  }

}
