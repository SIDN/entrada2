package nl.sidn.entrada2.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.StateService;
import nl.sidn.entrada2.service.StateService.APP_STATE;


@RestController
@RequestMapping("/state")
@Profile("controller")
@Slf4j
public class StateController implements BaseState{
  
  @Autowired
  private StateService stateService;

  @Override
  public ResponseEntity<APP_STATE> state() {
    return new ResponseEntity<>(stateService.getState(),HttpStatus.OK);
  }

  @Override
  public ResponseEntity<APP_STATE> start() {
    log.info("Change state to: {}", APP_STATE.RUNNING);
    
    stateService.setState(APP_STATE.RUNNING);
    return new ResponseEntity<>(stateService.getState(),HttpStatus.OK);
  }
  
  @Override
  public ResponseEntity<APP_STATE> stop() {
    log.info("Change state to: {}", APP_STATE.STOPPED);
    
    stateService.setState(APP_STATE.STOPPED);
    return new ResponseEntity<>(stateService.getState(),HttpStatus.OK);
  }

}
