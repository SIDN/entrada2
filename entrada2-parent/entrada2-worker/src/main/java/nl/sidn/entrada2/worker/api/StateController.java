package nl.sidn.entrada2.worker.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import nl.sidn.entrada2.worker.service.StateService;
import nl.sidn.entrada2.worker.service.StateService.APP_STATE;


@RestController
@RequestMapping("/state")
@ConditionalOnProperty( name = "entrada.mode", havingValue = "controller")
public class StateController implements BaseState{
  
  @Autowired
  private StateService stateService;

  @Override
  public ResponseEntity<APP_STATE> state() {
    return new ResponseEntity<>(stateService.getState(),HttpStatus.OK);
  }

  @Override
  public ResponseEntity<APP_STATE> start() {
    stateService.setState(APP_STATE.RUNNING);
    return new ResponseEntity<>(stateService.getState(),HttpStatus.OK);
  }
  
  @Override
  public ResponseEntity<APP_STATE> stop() {
    stateService.setState(APP_STATE.STOPPED);
    return new ResponseEntity<>(stateService.getState(),HttpStatus.OK);
  }

}
