package nl.sidn.entrada2.service;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.api.BaseWork;
import nl.sidn.entrada2.api.data.Work;
import nl.sidn.entrada2.api.data.WorkResult;
import nl.sidn.entrada2.load.DNSRowBuilder;
import nl.sidn.entrada2.load.DnsMetricValues;
import nl.sidn.entrada2.load.PacketJoiner;
import nl.sidn.entrada2.metric.HistoricalMetricManager;
import nl.sidn.entrada2.service.StateService.APP_STATE;
import nl.sidn.entrada2.util.CompressionUtil;
import nl.sidnlabs.pcap.PcapReader;

@Service
@Slf4j
public class WorkService {

  @Value("${entrada.inputstream.buffer:64}")
  private int bufferSizeConfig;

  @Autowired
  private S3FileService s3Service;
  @Autowired
  private PacketJoiner joiner;
  @Autowired
  private IcebergService writer;
  @Autowired
  private DNSRowBuilder rowBuilder;
  @Autowired
  private BaseWork workClient;
  @Autowired
  private HistoricalMetricManager metrics;

  @Value("#{${entrada.worker.sleep:10}*1000}")
  private int sleepMillis;

  @Value("#{${entrada.worker.stalled:10}*60*1000}")
  private int stalledMillis; 

  private boolean keepRunning = true;
  private long startOfWork;

  private MeterRegistry meterRegistry;
  private Counter workErrorCounter;

  public WorkService(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    workErrorCounter = meterRegistry.counter("worker.work.error");
  }

  @Async
  public CompletableFuture<Boolean> run() {

    while (keepRunning) {
      // make sure the loop never crashes otherwise processing stops
      startOfWork = 0;
      try {
        workBatch();
      } catch (Throwable e) {
        log.error("Error processing work", e);
        workErrorCounter.increment();
        sleep();
      }
    }

    return CompletableFuture.completedFuture(Boolean.TRUE);
  }

  private void workBatch() {

    ResponseEntity<Work> httpWork = workClient.work();

    if (httpWork.getStatusCode().is2xxSuccessful()) {

      Work w = httpWork.getBody();
      if (w.getState() == APP_STATE.STOPPED) {
        // if stopped then do not perform work
        if(log.isDebugEnabled()) {
          log.debug("Worker is stopped");
        }
        sleep();
        return;
      }
      
      log.info("Received new work: {}", w);

      startOfWork = System.currentTimeMillis();
      List<DataFile> dataFiles = process(w);
      long duration = System.currentTimeMillis() - startOfWork;
      // startOfWork is also use to check for stalled processing, reset after done with file
      startOfWork = 0;

      log.info("Processed file: {} in {}ms", w.getName(), duration);

      reportResults(w, dataFiles, duration);
      metrics.flush(w.getServer());
    } else {
      if(log.isDebugEnabled()) {
        log.debug("Received no work, feeling sleepy");
      }
      // no work sleep for while before trying again
      sleep();
    }
  }

  private void reportResults(Work work, List<DataFile> dataFiles, long duration) {

    Counter.builder("worker.work.files")
        .tags("server", work.getServer())
        .tags("location", work.getLocation())
        .register(meterRegistry)
        .increment();

    List<byte[]> byteList = new ArrayList<byte[]>();
    for (DataFile df : dataFiles) {
      byteList.add(SerializationUtils.serialize(df));
      log.info("Send datafile to controller for commit to table: {}", df.path());
    }

    WorkResult res = WorkResult.builder()
        .id(work.getId())
        .dataFiles(byteList)
        .time(duration)
        .filename(work.getName())
        .worker(System.getenv("HOSTNAME"))
        .build();

    log.info("Send work status: {}", res);

    workClient.status(work.getId(), res);
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



  /**
   * Check for stalled processing
   * 
   * @return true when currently processing file for > entrada.worker.stalled
   */
  public boolean isStalled() {
    return startOfWork > 0 && (System.currentTimeMillis() - startOfWork) > stalledMillis;
  }

  public List<DataFile> process(Work work) {

    List<DataFile> datafiles = null;

    Optional<InputStream> ois = s3Service.read(work.getBucket(), work.getKey());
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

        if(log.isDebugEnabled()) {
          log.debug("Extracted all data from file, now clear joiner cache");
        }
        
        // clear joiner cache, unmatched queries will get rcode -1
        joiner.clearCache().forEach(rd -> {
          Pair<GenericRecord, DnsMetricValues> rowPair = rowBuilder.build(rd, work.getServer(),
              work.getLocation(), writer.newGenericRecord());
          writer.write(rowPair.getKey());

          // update metrics
          metrics.update(rowPair.getValue());
        });

        if(log.isDebugEnabled()) {
          log.debug("Close parquet writer");
        }  

        datafiles = writer.close();
        
        if(log.isDebugEnabled()) {
          log.debug("Close pcap reader");
        }

        oreader.get().close();
      }

      Map<String, String> tags = Map.of("process_ts",
          LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), "process_ok", "yes");
      s3Service.tag(work.getBucket(), work.getKey(), tags);
    }

    return datafiles;
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
