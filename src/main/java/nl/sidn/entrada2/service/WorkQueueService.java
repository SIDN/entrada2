package nl.sidn.entrada2.service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.api.data.Work;
import nl.sidn.entrada2.api.data.WorkResult;
import nl.sidn.entrada2.data.FileArchiveRepository;
import nl.sidn.entrada2.data.FileInRepository;
import nl.sidn.entrada2.data.model.FileArchive;
import nl.sidn.entrada2.data.model.FileIn;
import nl.sidn.entrada2.service.StateService.APP_STATE;

@Service
@Slf4j
@Profile("controller")
public class WorkQueueService {

  @Value("${entrada.s3.bucket}")
  private String bucket;

  @Value("${entrada.directory.pcap}")
  private String directory;

  @Autowired
  private FileArchiveRepository fileArchiveRepository;

  @Autowired
  private FileInRepository fileInRepository;

  @Autowired
  private StateService stateService;

  private Queue<Work> workQueue = new ConcurrentLinkedQueue<>();

  private Gauge queueGauge;
  private Counter workServedCounter;

  public WorkQueueService(MeterRegistry meterRegistry) {
    queueGauge =
        Gauge.builder("controller.queue.size", workQueue, q -> q.size()).register(meterRegistry);
    workServedCounter =meterRegistry.counter("controller.worked.served.total");
  }

  public synchronized Optional<Work> getWork() {

    if (stateService.getState() != APP_STATE.RUNNING) {
      // do nothing when not in running state except
      // returning the state to the worker
      return Optional.of(Work.builder().state(stateService.getState()).build());
    }

    if (workQueue.isEmpty()) {

      log.debug("Work queue is empty load new work");

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
            .state(stateService.getState())
            .build();

        workQueue.add(w);

        log.info("Work queue size after loading: {}", workQueue);

      });

    }

    Work w = workQueue.poll();
    if (w != null) {
      // mark the file as served to a worker
      Optional<FileIn> ofi = fileInRepository.findByName(w.getName());
      ofi.ifPresent(fi -> {
        ofi.get().setServed(LocalDateTime.now());
        fileInRepository.save(ofi.get());
      });
      
      workServedCounter.increment();
      return Optional.ofNullable(w);
    }

    return Optional.empty();
  }

  @Transactional
  public synchronized void saveResult(WorkResult result) {

    Optional<FileIn> ofi = fileInRepository.findById(Long.valueOf(result.getId()));
    if (ofi.isPresent()) {
      FileIn fi = ofi.get();

      FileArchive fa = FileArchive.fromFileIn(fi)
          .served(LocalDateTime.now())
          .processed(LocalDateTime.now())
          .rows((int) result.getRows())
          .time((int) result.getTime())
          .build();

      fileArchiveRepository.save(fa);
      fileInRepository.delete(fi);

    }

  }

  // TODO: check if served files timeout and not getting moved to archive
}
