package nl.sidn.entrada2.worker.api;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@ConditionalOnProperty( name = "entrada.mode", havingValue = "worker")
public class TestController {
  
//  @Autowired
//  private WorkerClient client;
//  
//  @Autowired
//  private PcapReaderService pcapReaderService;

  @GetMapping(path = "/test", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Work> test() {

//    ResponseEntity<Work> httpWork = client.work();
//    if (httpWork.getStatusCode().is2xxSuccessful()) {
//
//      pcapReaderService.process(httpWork.getBody());
//      
//      return new ResponseEntity<>(httpWork.getBody(), HttpStatus.OK);
//    }

    return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
  }


}