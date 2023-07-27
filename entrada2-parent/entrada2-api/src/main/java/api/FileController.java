package api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import api.service.FileStorageService;


@RestController
public class FileController {
  
  
  @Autowired
  private FileStorageService fileStorageService;


  @PostMapping(
      path = "/upload",
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> upload(@RequestParam("server") String server,
      @RequestParam("location") String location,
      @RequestParam("file") MultipartFile file) {

    if (fileStorageService.save(server, location, file)) {
      return new ResponseEntity<>("ok...", HttpStatus.OK);
    }

    return new ResponseEntity<>("Fail...", HttpStatus.INTERNAL_SERVER_ERROR);
  }


}
