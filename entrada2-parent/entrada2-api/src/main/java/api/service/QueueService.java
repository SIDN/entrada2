package api.service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import api.data.FileInRepository;
import api.data.model.FileIn;
import nl.sidn.entrada.data.Work;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class QueueService {
  
  @Value("${aws.endpoint}")
  private String endpoint;
  
  @Value("${aws.bucket}")
  private String bucket;
  
  @Value("${aws.directory}")
  private String directory;

  @Autowired
  private FileInRepository fileInRepository;

  private Queue<Work> workQueue = new ConcurrentLinkedQueue<>();

  public synchronized Optional<Work> getWork() {

    if (workQueue.isEmpty()) {

      Pageable pageable = PageRequest.of(0, 1000, Sort.by("created").ascending());
      fileInRepository.findByServedIsNull(pageable).forEach(row -> {
        Work w = Work.builder()
            .name(row.getName())
            .bucket(bucket)
            .key(directory + "/" + row.getName())
            .server(row.getServer())
            .location(row.getLocation())
            .size(row.getSize())
            .build();

        workQueue.add(w);

      });

    }

    Work w = workQueue.poll();
    if(w != null) {
      Optional<FileIn> ofi = fileInRepository.findByName(w.getName());
      ofi.ifPresent( fi -> {
        ofi.get().setServed(LocalDateTime.now());
        fileInRepository.save(ofi.get());
      });

    }

    return Optional.ofNullable(w);

  }
  
  //TODO: check if served files timeout and not getting moved to archive
}
