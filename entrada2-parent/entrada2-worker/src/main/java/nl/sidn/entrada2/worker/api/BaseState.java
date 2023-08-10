package nl.sidn.entrada2.worker.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import nl.sidn.entrada2.worker.service.StateService.APP_STATE;

@FeignClient(name="stateClient")
public interface BaseState {
  
  @PutMapping(path = "/start")
  public ResponseEntity<APP_STATE> start();
  
  @PutMapping(path = "/stop")
  public ResponseEntity<APP_STATE> stop() ;
  
  @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<APP_STATE> state() ;
  
}
