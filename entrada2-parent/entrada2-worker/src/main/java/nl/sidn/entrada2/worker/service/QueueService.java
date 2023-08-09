package nl.sidn.entrada2.worker.service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.worker.api.Work;
import nl.sidn.entrada2.worker.api.WorkResult;
import nl.sidn.entrada2.worker.data.FileArchiveRepository;
import nl.sidn.entrada2.worker.data.FileInRepository;
import nl.sidn.entrada2.worker.data.model.FileArchive;
import nl.sidn.entrada2.worker.data.model.FileIn;

@Service
@Slf4j
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
@ConditionalOnProperty( name = "entrada.mode", havingValue = "controller")
public class QueueService {
  
  @Value("${aws.endpoint}")
  private String endpoint;
  
  @Value("${aws.bucket}")
  private String bucket;
  
  @Value("${aws.directory.pcap}")
  private String directory;
  
  @Autowired
  private FileArchiveRepository fileArchiveRepository;

  @Autowired
  private FileInRepository fileInRepository;

  private Queue<Work> workQueue = new ConcurrentLinkedQueue<>();

  public synchronized Optional<Work> getWork() {

    if (workQueue.isEmpty()) {

      Pageable pageable = PageRequest.of(0, 1000, Sort.by("created").ascending());
      fileInRepository.findByServedIsNull(pageable).forEach(row -> {
        Work w = Work.builder()
            .id(row.getId().longValue())
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
      // mark the file as served to a worker
      Optional<FileIn> ofi = fileInRepository.findByName(w.getName());
      ofi.ifPresent( fi -> {
        ofi.get().setServed(LocalDateTime.now());
        fileInRepository.save(ofi.get());
      });

    }

    return Optional.ofNullable(w);

  }

  
  @Transactional
  public synchronized void saveResult(WorkResult result) {

    Optional<FileIn> ofi = fileInRepository.findById(Long.valueOf(result.getId()));
    if(ofi.isPresent()) {
      FileIn fi = ofi.get();
      
      FileArchive fa = FileArchive.fromFileIn(fi)
      .served(LocalDateTime.now())
      .processed(LocalDateTime.now())
      .rows((int)result.getRows())
      .time((int)result.getTime())
      .build();
      
       fileArchiveRepository.save(fa);    
       fileInRepository.delete(fi);

    }
   
  }
  
  //TODO: check if served files timeout and not getting moved to archive
}
