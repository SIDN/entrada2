package nl.sidn.entrada2.worker.service;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
  
  @Autowired
  private Schema schema;
  @Autowired
  private RESTCatalog catalog;
  
  private Table table;
 // private SortOrder sortOrder;
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
          .set("write.parquet.compression-codec", "snappy")
          .set("write.metadata.delete-after-commit.enabled", "true")
          .set("write.metadata.previous-versions-max", "50")
          .commit();
      
      this.table.replaceSortOrder()
        .asc("domainname")
        .commit();
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
    
    fileAppenderFactory.set("write.parquet.compression-codec", "snappy");
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
        log.error("Cannot create writer", e);
        return;
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
      throw new RuntimeException("Cannot add new data files to table",e);
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
      wrapper = new InternalRecordWrapper(table.schema().asStruct());
    }

    @Override
    protected PartitionKey partition(GenericRecord record) {
      partitionKey.partition(wrapper.wrap(record));
      return partitionKey;
    }

  }
  
//  private class SortingGenericAppenderFactory extends GenericAppenderFactory{
//    
//    private final Map<String, String> config = Maps.newHashMap();
//
//    public SortingGenericAppenderFactory(Schema schema, PartitionSpec spec) {
//      super(schema, spec);
//    }
//    
//    public GenericAppenderFactory set(String property, String value) {
//      super.set(property, value);
//      
//      config.put(property, value);
//      return this;
//    }
//
//    public GenericAppenderFactory setAll(Map<String, String> properties) {
//      super.setAll(properties);
//      
//      config.putAll(properties);
//      return this;
//    }
//    
//    @Override
//    public FileAppender<Record> newAppender(OutputFile outputFile, FileFormat fileFormat) {
//      MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
//
//        if(FileFormat.PARQUET == fileFormat) {
//
//            try {
//              return Parquet.write(outputFile)
//                  .schema(schema)
//                  .createWriterFunc(GenericParquetWriter::buildWriter)
//                  .setAll(config)
//                  .metricsConfig(metricsConfig)
//                  .overwrite()
//                  .build();
//            } catch (IOException e) {
//             throw new RuntimeException("Error creating Parquet writer", e);
//            }
//            
//        }
//    
//        return super.newAppender(outputFile, fileFormat);
//    
//     }
//  }

}


