package nl.sidn.entrada2.worker.service;

import java.time.LocalDateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.worker.data.FileArchiveRepository;
import nl.sidn.entrada2.worker.data.FileInRepository;
import nl.sidn.entrada2.worker.data.model.FileArchive;
import nl.sidn.entrada2.worker.data.model.FileIn;

@Service
@Slf4j
@ConditionalOnProperty( name = "entrada.mode", havingValue = "controller")
public class UploadService {
  
  @Value("${aws.bucket}")
  private String bucket;
  
  @Value("${aws.directory.pcap}")
  private String directory;

  @Autowired
  private AmazonS3 amazonS3;

  @Autowired
  private FileInRepository fileRepository;
  
  public FileIn save(String server, String location, MultipartFile file) {
    log.info("Uploading file: {}", file.getOriginalFilename());

    int fileSize = -1;
    try {

      log.info("Size of file: {}", file.getInputStream().available());

      ObjectMetadata objectMetadata = new ObjectMetadata();
      fileSize = file.getInputStream().available();
      objectMetadata.setContentLength(fileSize);

      amazonS3.putObject(bucket, directory + "/" + file.getOriginalFilename(), file.getInputStream(),
          objectMetadata);

    } catch (Exception e) {
      throw new RuntimeException("Error during uplouding to s3", e);
    }

    FileIn fin = FileIn.builder()
        .name(file.getOriginalFilename())
        .created(LocalDateTime.now())
        .server(server)
        .location(location)
        .size(fileSize)
        .build();
    return fileRepository.save(fin);
  }

  
}
