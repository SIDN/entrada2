package nl.sidn.entrada2.api;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import nl.sidn.entrada2.api.data.Work;
import nl.sidn.entrada2.api.data.WorkResult;

public interface BaseWork {

  @PutMapping(path = "/{id}/status", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Void> status(@PathVariable long id,  @RequestBody WorkResult result);

  @GetMapping(path = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Work> work();
  
}
