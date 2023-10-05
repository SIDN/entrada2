package nl.sidn.entrada2.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.rest.RESTCatalog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;


@Service
@Slf4j
public class IcebergService {

  @Value("#{${entrada.parquet.file.max-size:256} * 1024 * 1024}")
  private int maxFileSizeMegabyte;

  @Value("${iceberg.compression}")
  private String compressionAlgo;

  @Autowired
  private Table table;

  private GenericRecord genericRecord;
  private WrappedPartitionedFanoutWriter partitionedFanoutWriter;
  
  private BlockingQueue<DataFile> datafileQueue = new LinkedBlockingQueue<>();

  @PostConstruct
  public void initialize() {
    this.genericRecord = GenericRecord.create(table.schema());
  }


  private void createWriter() throws IOException {

    // make sure to also use the spec when creating a GenericAppenderFactory otherwise
    // iceberg will not generate the partitioning metadata
    GenericAppenderFactory fileAppenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());

    fileAppenderFactory.set("write.parquet.compression-codec", compressionAlgo);
    fileAppenderFactory.set("write.metadata.delete-after-commit.enabled", "true");
    fileAppenderFactory.set("write.metadata.previous-versions-max", "50");
    
    fileAppenderFactory.set("http-client.urlconnection.socket-timeout-ms", "5000");
    fileAppenderFactory.set("http-client.urlconnection.connection-timeout-ms", "5000");


    int partitionId = 1, taskId = 1;
    OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(FileFormat.PARQUET).build();

    // the WrappedPartitionedFanoutWriter will create multiple partitions if data in the pcap is
    // for multiple days
    partitionedFanoutWriter =
        new WrappedPartitionedFanoutWriter(table, fileAppenderFactory, outputFileFactory);

  }

  public void write(GenericRecord record) {

    if (partitionedFanoutWriter == null) {

      try {
        createWriter();
      } catch (IOException e) {
        throw new RuntimeException("Cannot create writer", e);
      }

    }
    try {
      partitionedFanoutWriter.write(record);
    } catch (Exception e) {
      log.error("Error writing row: {}", record);
    }
  }

  public List<DataFile> close() {

    try {
      List<DataFile> files = Arrays.stream(partitionedFanoutWriter.dataFiles()).toList();
      partitionedFanoutWriter = null;
      return files;
    } catch (Exception e) {
      log.error("Creating datafiles failed", e);
    }

    return Collections.emptyList();
  }

  public void commit() {
    
    List<DataFile> dataFiles = new ArrayList<>();
    while (!datafileQueue.isEmpty()) {
      dataFiles.add(datafileQueue.poll());
    }

    if (!dataFiles.isEmpty()) {
      log.info("Commit {} new datafiles", dataFiles.size());

      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        log.info("Add file: " + dataFile.path());
        appendFiles.appendFile(dataFile);
      }

      appendFiles.commit();
    }
  }


  public GenericRecord newGenericRecord() {
    return genericRecord.copy();
  }

  
  public void addDataFile(DataFile f) {
    try {
      datafileQueue.put(f);
    } catch (InterruptedException e) {
      log.error("Adding datafile to commit queue failed",e);
    }
  }

  public boolean isDataFileToCommit() {
    return !datafileQueue.isEmpty();
  }

  private class WrappedPartitionedFanoutWriter extends PartitionedFanoutWriter<GenericRecord> {

    private InternalRecordWrapper wrapper;
    private PartitionKey partitionKey;

    public WrappedPartitionedFanoutWriter(Table table, FileAppenderFactory appenderFactory,
        OutputFileFactory fileFactory) {
      super(table.spec(), FileFormat.PARQUET, appenderFactory, fileFactory, table.io(),
          maxFileSizeMegabyte);

      partitionKey = new PartitionKey(table.spec(), table.spec().schema());

      // need to use wrapper for conversion datetime/long see:
      // https://github.com/apache/iceberg/issues/6510#issuecomment-1377570948
      wrapper = new InternalRecordWrapper(table.schema().asStruct());
    }

    @Override
    protected PartitionKey partition(GenericRecord record) {
      partitionKey.partition(wrapper.wrap(record));
      return partitionKey;
    }
  }
  
  

}


