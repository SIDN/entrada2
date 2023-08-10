package nl.sidn.entrada2.worker.service;

import org.springframework.stereotype.Service;
import lombok.Getter;
import lombok.Setter;

@Service
@Getter
@Setter
public class StateService {
  
  public enum APP_STATE {
    RUNNING, STOPPED
  };

  
  private APP_STATE state = APP_STATE.RUNNING;
  

}
