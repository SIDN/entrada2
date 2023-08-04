package nl.sidn.entrada2.worker.api;

import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import nl.sidn.entrada2.worker.service.QueueService;

@RestController
@RequestMapping("/work")
@ConditionalOnProperty( name = "entrada.mode", havingValue = "controller")
public class WorkController implements BaseWork {

  @Autowired
  private QueueService queueService;


  @Override
  public ResponseEntity<Void> status(WorkResult result) {
    
    if(queueService.saveResult(result)) {
      return new ResponseEntity<>(HttpStatus.OK);
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Override
  public ResponseEntity<Work> work() {
    
    Optional<Work> ow = queueService.getWork();
    if(ow.isPresent()) {
      return new ResponseEntity<>(ow.get(), HttpStatus.OK);
    }

    return new ResponseEntity<>(HttpStatus.NOT_FOUND);
  }

}
