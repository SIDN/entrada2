package nl.sidn.entrada2.worker.service;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.data.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.worker.api.BaseWork;
import nl.sidn.entrada2.worker.api.data.Work;
import nl.sidn.entrada2.worker.api.data.WorkResult;
import nl.sidn.entrada2.worker.load.DNSRowBuilder;
import nl.sidn.entrada2.worker.load.DnsMetricValues;
import nl.sidn.entrada2.worker.load.PacketJoiner;
import nl.sidn.entrada2.worker.metric.HistoricalMetricManager;
import nl.sidn.entrada2.worker.service.StateService.APP_STATE;
import nl.sidn.entrada2.worker.util.CompressionUtil;
import nl.sidnlabs.pcap.PcapReader;

@Service
@Slf4j
public class WorkService {

  @Value("${entrada.inputstream.buffer:64}")
  private int bufferSizeConfig;

  @Autowired
  private S3FileService s3FileService;
  @Autowired
  private PacketJoiner joiner;
  @Autowired
  private IcebergWriterService writer;
  @Autowired
  private DNSRowBuilder rowBuilder;
  @Autowired
  private BaseWork workClient;
  @Autowired
  private HistoricalMetricManager metrics;

  @Value("#{${entrada.worker.sleep:10}*1000}")
  private int sleepMillis;

  private boolean keepRunning = true;
 
  @Async
  public CompletableFuture<Boolean> run() {

    while (keepRunning) {
      // make sure the loop never crashes otherwise processing stops
      try {
        workBatch();
      }catch (Exception e) {
        log.error("Error processing work", e);
        sleep();
      }
    }

    return CompletableFuture.completedFuture(Boolean.TRUE);
  }
  
  private void workBatch() {

      ResponseEntity<Work> httpWork = workClient.work();

      if (httpWork.getStatusCode().is2xxSuccessful()) {

        Work w = httpWork.getBody();
        log.info("Received work: {}", w);
        
        if(w.getState()== APP_STATE.STOPPED) {
          // if stopped then do not perform work
          log.debug("Worker is stopped");
          sleep();
          return;
        }

        long start = System.currentTimeMillis();
        long rows = process(w);
        long duration = System.currentTimeMillis() - start;

        log.info("Processed file: {} in {}ms", w.getName(), duration);

        reportResults(w.getId(), rows, duration);
        metrics.flush(w.getServer());
      }

      log.info("Received no work, feeling sleepy");
      // no work sleep for while before trying again
      sleep();
  }

  private void reportResults(long id, long rows, long duration) {
    WorkResult res = WorkResult.builder()
        .id(id)
        .rows(rows)
        .time(duration)
        .build();

    log.info("Send work status: {}", res);

    workClient.status(id, res);
  }

  public void stop() {
    keepRunning = false;
  }

  private void sleep() {
    try {
      Thread.sleep(sleepMillis);
    } catch (InterruptedException e) {
      log.error("Interupted while having a nice sleep", e);
    }
  }



  public long process(Work work) {

    long rowCount = 0;

    Optional<InputStream> ois = s3FileService.read(work.getBucket(), work.getKey());
    if (ois.isPresent()) {
      Optional<PcapReader> oreader = createReader(work.getName(), ois.get());
      if (oreader.isPresent()) {
        oreader.get().stream().forEach(p -> {

          joiner.join(p).forEach(rd -> {
            Pair<GenericRecord, DnsMetricValues> rowPair = rowBuilder.build(rd, work.getServer(),
                work.getLocation(), writer.newGenericRecord());
            writer.write(rowPair.getKey());

            // update metrics
            metrics.update(rowPair.getValue());

          });
        });

        rowCount = writer.close();
      }
    }

    return rowCount;
  }


  private Optional<PcapReader> createReader(String file, InputStream is) {

    try {
      InputStream decompressor =
          CompressionUtil.getDecompressorStreamWrapper(is, bufferSizeConfig * 1024, file);
      return Optional.of(new PcapReader(new DataInputStream(decompressor), null, true,
          file, false));
    } catch (IOException e) {
      log.error("Error creating pcap reader for: " + file, e);
      try {
        is.close();
      } catch (Exception e2) {
        log.error("Cannot close inputstream, maybe it was not yet opened");
      }
    }
    return Optional.empty();
  }

}
