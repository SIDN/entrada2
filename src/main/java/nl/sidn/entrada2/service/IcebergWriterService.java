package nl.sidn.entrada2.service;

import java.io.IOException;
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
public class IcebergWriterService {
  
  @Value("#{${entrada.parquet.file.max-size:256} * 1024 * 1024}")
  private int maxFileSizeMegabyte;
  
  @Value("${iceberg.compression}")
  private String compressionAlgo;
  
  @Autowired
  private Schema schema;
  @Autowired
  private RESTCatalog catalog;
  
  private Table table;
  private PartitionSpec spec;
  private GenericRecord genericRecord;
  private WrappedPartitionedFanoutWriter partitionedFanoutWriter;

  @PostConstruct
  public void initialize() {

    this.spec = PartitionSpec.builderFor(schema)
        .day("time", "day")
        .identity("server")
        .build();
    
    Namespace namespace = Namespace.of("entrada");
    if (!catalog.namespaceExists(namespace)) {
      catalog.createNamespace(namespace);
    }

    TableIdentifier tableId = TableIdentifier.of(namespace, "dns");
    if (!catalog.tableExists(tableId)) {
      this.table = catalog.createTable(tableId, schema, spec);
      this.table
          .updateProperties()
          .set("write.parquet.compression-codec", compressionAlgo)
          .set("write.metadata.delete-after-commit.enabled", "true")
          .set("write.metadata.previous-versions-max", "50")
          .commit();
    
      // Sort disabled as writer does not sort 
      // enable again when writer supports sort
//      this.table.replaceSortOrder()
//        .asc("domainname")
//        .commit();
    } else {
      this.table = catalog.loadTable(tableId);
    }

    this.genericRecord = GenericRecord.create(this.table.schema());
  }


  private void createWriter() throws IOException {

    // make sure to also use the spec when creating a GenericAppenderFactory otherwise
    // iceberg will not generate the partitioning metadata
    GenericAppenderFactory fileAppenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());
    
    fileAppenderFactory.set("write.parquet.compression-codec", compressionAlgo);
    fileAppenderFactory.set("write.metadata.delete-after-commit.enabled", "true");
    fileAppenderFactory.set("write.metadata.previous-versions-max", "50");


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

  public long close() {
    long rows = 0;
    
    try {
      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : partitionedFanoutWriter.dataFiles()) {
        rows = rows + dataFile.recordCount();
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();
    } catch (Exception e) {
      log.error("Cannot add new data files to table",e);
    }finally {
      partitionedFanoutWriter = null;
    }
    
    return rows;
  }


  public GenericRecord newGenericRecord() {
    return genericRecord.copy();
  }


  private class WrappedPartitionedFanoutWriter extends PartitionedFanoutWriter<GenericRecord> {

    private InternalRecordWrapper wrapper;
    private PartitionKey partitionKey;

    public WrappedPartitionedFanoutWriter(Table table, FileAppenderFactory appenderFactory,
        OutputFileFactory fileFactory) {
      super(table.spec(), FileFormat.PARQUET, appenderFactory, fileFactory, table.io(), maxFileSizeMegabyte);

      partitionKey = new PartitionKey(table.spec(), table.spec().schema());
      
      //need to use wrapper for conversion datetime/long see:
      //https://github.com/apache/iceberg/issues/6510#issuecomment-1377570948
      wrapper = new InternalRecordWrapper(table.schema().asStruct());
    }

    @Override
    protected PartitionKey partition(GenericRecord record) {
      partitionKey.partition(wrapper.wrap(record));
      return partitionKey;
    }

  }
 

}


