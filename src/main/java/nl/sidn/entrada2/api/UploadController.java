package nl.sidn.entrada2.api;

import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.data.model.FileIn;
import nl.sidn.entrada2.service.StateService;
import nl.sidn.entrada2.service.StateService.APP_STATE;
import nl.sidn.entrada2.service.UploadService;

@Slf4j
@RestController
@Profile("controller")
public class UploadController {

  @Autowired
  private StateService stateService;
  @Autowired
  private UploadService fileStorageService;


  @PostMapping(
      path = "/upload",
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<FileIn> upload(@RequestParam("server") String server,
      @RequestParam("location") String location,
      @RequestParam("file") MultipartFile file) {
    
    if(stateService.getState() == APP_STATE.RUNNING) {

      Optional<FileIn> of = fileStorageService.save(server, location, file);
      if(of.isPresent()) {
        return new ResponseEntity<>(of.get(), HttpStatus.OK);
      }
    
      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    // not allowed to upload data when not in running state
    log.error("Received upload request, but current state {} does not allow uploading new pcap files", stateService.getState());
    return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
    
  }


}
