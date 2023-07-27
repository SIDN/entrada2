package nl.sidn.entrada.api;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import nl.sidn.entrada.data.Work;

public interface BaseWork {

  
  @PostMapping(path = "/report", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> report();

  @GetMapping(path = "/work", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Work> work();
  
  
}
