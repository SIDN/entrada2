package nl.sidn.entrada2.worker.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import nl.sidn.entrada2.worker.data.model.FileIn;
import nl.sidn.entrada2.worker.service.UploadService;

@RestController
@ConditionalOnProperty(name = "entrada.mode", havingValue = "controller")
public class UploadController {


  @Autowired
  private UploadService fileStorageService;


  @PostMapping(
      path = "/upload",
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<FileIn> upload(@RequestParam("server") String server,
      @RequestParam("location") String location,
      @RequestParam("file") MultipartFile file) {

    FileIn f = fileStorageService.save(server, location, file);
    return new ResponseEntity<>(f, HttpStatus.OK);
  }


}
