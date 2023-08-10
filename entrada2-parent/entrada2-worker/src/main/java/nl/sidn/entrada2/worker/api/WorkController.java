package nl.sidn.entrada2.worker.api;

import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.worker.api.data.Work;
import nl.sidn.entrada2.worker.api.data.WorkResult;
import nl.sidn.entrada2.worker.service.WorkQueueService;

@RestController
@Slf4j
@RequestMapping("/work")
@ConditionalOnProperty( name = "entrada.mode", havingValue = "controller")
public class WorkController implements BaseWork {

  @Autowired
  private WorkQueueService queueService;


  @Override
  public ResponseEntity<Void> status(long id, WorkResult result) {
    log.info("Received work status: {}", result);
    
    queueService.saveResult(result);
    return new ResponseEntity<>(HttpStatus.OK);

  }

  @Override
  public ResponseEntity<Work> work() {
    if(log.isDebugEnabled()) {
      log.debug("Received request for work");
    }
    
    Optional<Work> ow = queueService.getWork();
    if(ow.isPresent()) {
      return new ResponseEntity<>(ow.get(), HttpStatus.OK);
    }

    return new ResponseEntity<>(HttpStatus.NOT_FOUND);
  }

}
