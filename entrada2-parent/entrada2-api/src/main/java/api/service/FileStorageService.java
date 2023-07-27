package api.service;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import api.data.FileArchiveRepository;
import api.data.FileInRepository;
import api.data.model.FileArchive;
import api.data.model.FileIn;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FileStorageService {
  
  @Value("${aws.bucket}")
  private String bucket;
  
  @Value("${aws.directory}")
  private String directory;

  @Autowired
  private AmazonS3 amazonS3;

  @Autowired
  private FileInRepository fileRepository;
  @Autowired
  private FileArchiveRepository fileArchiveRepository;

  public boolean save(String server, String location, MultipartFile file) {
    log.info("Save file: {}", file.getOriginalFilename());

    int fileSize = -1;
    try {

      log.info("Size of file: {}", file.getInputStream().available());

      ObjectMetadata objectMetadata = new ObjectMetadata();
      fileSize = file.getInputStream().available();
      objectMetadata.setContentLength(fileSize);

      amazonS3.putObject(bucket, directory + "/" + file.getOriginalFilename(), file.getInputStream(),
          objectMetadata);

    } catch (Exception e) {

      throw new RuntimeException(e.getMessage());
    }

    FileIn fin = FileIn.builder()
        .name(file.getOriginalFilename())
        .created(LocalDateTime.now())
        .server(server)
        .location(location)
        .size(fileSize)
        .build();
    fileRepository.save(fin);

    
    fileArchiveRepository.save(FileArchive.fromFileIn(fin)
        .served(LocalDateTime.now())
        .processed(LocalDateTime.now())
        .rows(1)
        .time(2)
        .size(fileSize)
        .parquetFile("parquetfile")
        .build());
    
    return true;
  }

  
}
