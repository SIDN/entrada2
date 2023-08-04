package nl.sidn.entrada2.worker.api;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/state")
@ConditionalOnProperty( name = "entrada.mode", havingValue = "controller")
public class StateController implements BaseState{
  

  @Override
  public ResponseEntity<State> state() {
    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Override
  public ResponseEntity<Void> start() {
    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }
  
  @Override
  public ResponseEntity<Void> stop() {
    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

}
