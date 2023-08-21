package nl.sidn.entrada2.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.data.FileInRepository;
import nl.sidn.entrada2.data.model.FileIn;

@Service
@Slf4j
@Profile("controller")
public class UploadService {
  
  @Value("${entrada.s3.bucket}")
  private String bucket;
  
  @Value("${entrada.directory.pcap}")
  private String directory;

  @Autowired
  private S3FileService s3FileService;

  @Autowired
  private FileInRepository fileRepository;
  
  @Autowired
  private MeterRegistry meterRegistry;

  public Optional<FileIn> save(String server, String location, MultipartFile file) {
    log.info("Uploading file: {}", file.getOriginalFilename());
    
    Counter.builder("controlle.files.upload.total")
    .tags("server", server)
    .tags("location", location)
    .register(meterRegistry)
    .increment();

    
    String key = directory + "/" + file.getOriginalFilename();
    if(s3FileService.save(bucket, key, file)) {
      Map<String, String> tags = Map.of("server", server, "location", location);
      s3FileService.tag(bucket, key, tags);
      
      FileIn fin = FileIn.builder()
          .name(file.getOriginalFilename())
          .created(LocalDateTime.now())
          .server(server)
          .location(location)
          .size(file.getSize())
          .build();
      return Optional.of(fileRepository.save(fin));
    }
    
    Counter.builder("controller.file.upload.error")
    .tags("server", server)
    .tags("location", location)
    .register(meterRegistry)
    .increment();

    return Optional.empty();
  }

  
}
