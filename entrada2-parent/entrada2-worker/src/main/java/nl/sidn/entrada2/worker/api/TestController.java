package nl.sidn.entrada2.worker.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import com.amazonaws.services.transfer.model.S3FileLocation;
import nl.sidn.entrada.data.Work;
import nl.sidn.entrada2.worker.client.WorkerClient;
import nl.sidn.entrada2.worker.service.PcapReaderService;
import nl.sidn.entrada2.worker.service.S3FileService;


@RestController
public class TestController {
  
  @Autowired
  private WorkerClient client;
  
  @Autowired
  private PcapReaderService pcapReaderService;

  @GetMapping(path = "/test1", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Work> test() {

    ResponseEntity<Work> httpWork = client.work();
    if (httpWork.getStatusCode().is2xxSuccessful()) {

      pcapReaderService.process(httpWork.getBody());
      
      return new ResponseEntity<>(httpWork.getBody(), HttpStatus.OK);
    }

    return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
  }


}