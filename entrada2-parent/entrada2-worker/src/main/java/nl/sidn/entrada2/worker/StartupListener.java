package nl.sidn.entrada2.worker;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.worker.service.PcapReaderService;

@Component
@Slf4j
@ConditionalOnProperty( name = "entrada.mode", havingValue = "worker")
public class StartupListener {


  
  @Autowired
  private PcapReaderService pcapReaderService;
  

  
  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent event) {
      log.info("Start worker thread");
      pcapReaderService.run();
  }
  


}
