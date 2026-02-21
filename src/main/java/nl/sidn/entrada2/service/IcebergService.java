package nl.sidn.entrada2.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.types.Types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.load.FieldEnum;
import nl.sidn.entrada2.service.messaging.LeaderQueue;

@Service
@Slf4j
public class IcebergService {

	@Value("${iceberg.compression}")
	private String compressionAlgo;

	@Value("#{${iceberg.parquet.dictionary-max-mb:2}*1024*1024}")
	private int parquetDictMaxBytes;
	
	// 10 millions recors is about 256MB without rdata enabled.
	@Value("${iceberg.parquet.min-records:1000000}")
	private int parquetMinRecords;

	@Value("${iceberg.table.bloomfilter:true}")
	private boolean enableBloomFilter;
	
	@Value("#{${iceberg.parquet.bloomfilter-max-size-mb:1} * 1024 * 1024}")
	private int parquetBloomFilterMaxBytes;
	
	@Value("${iceberg.metadata.version.max:100}")
	private int metadataVersionMax;

	// number of recs written to current file
	private long currentRecCount;

	@Autowired
	private Table table;
	
	// Store schemas instead of template records to avoid copy overhead
	private StructType recordSchema;
	private StructType rdataSchema;
	private PartitionedFanoutWriter<GenericRecord> partitionedFanoutWriter;

	private int writeErrors;

	private boolean flush = false;

	@Autowired
	private LeaderQueue leaderQueue;

	@PostConstruct
	public void initialize() {
		this.recordSchema = table.schema().asStruct();
		this.rdataSchema = table.schema().findType(49).asStructType();
	}

	private PartitionedFanoutWriter<GenericRecord> createWriter() throws IOException {
		
		currentRecCount = 0;

		// make sure to also use the spec when creating a GenericAppenderFactory
		// otherwise iceberg will not generate the partitioning metadata
		GenericAppenderFactory fileAppenderFactory = new GenericAppenderFactory(table.schema(), table.spec());

		fileAppenderFactory.set(TableProperties.PARQUET_COMPRESSION, compressionAlgo);
		fileAppenderFactory.set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
		fileAppenderFactory.set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, String.valueOf(metadataVersionMax));

		//fileAppenderFactory.set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(maxFileSizeBytes));
		
		if (enableBloomFilter) {
			fileAppenderFactory.set(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + FieldEnum.dns_domainname.name(), "true");
			fileAppenderFactory.set(TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES, parquetBloomFilterMaxBytes + "");
		}

		// use small dict size otherwize the domainname column will use dictionary
		// ecoding and parquet
		// will only write a bloomfilter when dict encoding is NOT used for the column.
		// not using dict encoding for domainname will increase size of file but at
		// query time we can
		// potentially skip many files/rowgroups that do not contain records for a
		// domain
		fileAppenderFactory.set(TableProperties.PARQUET_DICT_SIZE_BYTES, "" + parquetDictMaxBytes);

		int partitionId = 1, taskId = 1;
		OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
				.format(FileFormat.PARQUET).build();

		// the WrappedPartitionedFanoutWriter will create multiple partitions if data in
		// the pcap is for multiple days
		return new WrappedPartitionedFanoutWriter(table, fileAppenderFactory, outputFileFactory);

	}

	private void clear() {
		flush = false;
		currentRecCount = 0;
		writeErrors = 0;
		partitionedFanoutWriter = null;
	}

	public void write(GenericRecord record) {

		currentRecCount++;
		
		if(currentRecCount % 100000 == 0) {
			log.info("Processed {} rows", currentRecCount);
		}

		if (partitionedFanoutWriter == null) {
			
			if(log.isDebugEnabled()) {
				log.debug("Create new writer");
			}

			try {
				partitionedFanoutWriter = createWriter();
			} catch (IOException e) {
				
				log.error("Error while creating writer", e);
				
				throw new RuntimeException("Cannot create writer", e);
			}
		}
		
		try {
			partitionedFanoutWriter.write(record);
		} catch (IOException e) {
			// debug log error only, no error per row
			writeErrors++;
			if(log.isDebugEnabled()) {
				log.debug("Error writing row: {}", record, e);
			}
		}
	}

	private List<DataFile> close() {
		
		if(log.isDebugEnabled()) {
			log.debug("Closing writer");
		}

		log.info("Close Iceberg writer, records: {} errors: {}", currentRecCount, writeErrors);
		//writePageBatch();

		if (partitionedFanoutWriter != null) {
			try {
				// close writer and get datafiles
				List<DataFile> files = Arrays.stream(partitionedFanoutWriter.dataFiles()).toList();
				partitionedFanoutWriter = null;
				
				if(log.isDebugEnabled()) {
					log.debug("Result was {} data files", files.size());
				}
				
				return files;
			} catch (Exception e) {
				log.error("Creating datafiles failed", e);
			}
		}
		return Collections.emptyList();
	}

	public void commit(DataFile dataFile) {	
		commit(Collections.singletonList(dataFile));
	}
	
	public void commit(List<DataFile> dataFiles) {
		AppendFiles appendFiles = null;
		
		if(dataFiles.size() > 0) {
			appendFiles = table.newAppend();
		}
		
		for(DataFile dataFile: dataFiles) {
			log.info("Append Iceberg datafile, rows: {} path: {}", dataFile.recordCount(), dataFile.location());
			if(appendFiles != null) {
				appendFiles.appendFile(dataFile);
			}
		}
		
		if(dataFiles.size() > 0 && appendFiles != null) {
			appendFiles.commit();
		}
	}
	
	public void commit(boolean force) {
		if(force || flush || currentRecCount > parquetMinRecords) {
			// even though we have a check for parquetMinRecords there can still be some small
			// parquet files when processing data around date boundary
			commit();
			// remove any old metrics and writers
			clear();
		}
	}

	public void commit() {

		// close the current open parquet output file
		log.info("Commit - currentRecCount: {}", currentRecCount);
		for (DataFile dataFile : close()) {
			if(log.isDebugEnabled()) {
				log.debug("Send datafile: {}", dataFile);
			}
			// send new datafile to leader
			leaderQueue.send(dataFile);
		}
	}

	public GenericRecord newGenericRecord() {
		// Create fresh record - faster than copying empty template (avoids deep copy loop)
		return GenericRecord.create(recordSchema);
	}
	
	public GenericRecord newRdataGenericRecord() {
		// Create fresh record - faster than copying empty template (avoids deep copy loop)
		return GenericRecord.create(rdataSchema);
	}

	private class WrappedPartitionedFanoutWriter extends PartitionedFanoutWriter<GenericRecord> {

		private InternalRecordWrapper wrapper;
		private PartitionKey partitionKey;

		@SuppressWarnings("unchecked")
		public WrappedPartitionedFanoutWriter(Table table, @SuppressWarnings("rawtypes") FileAppenderFactory fileAppenderFactory,
				OutputFileFactory fileFactory) {
			super(table.spec(), FileFormat.PARQUET, fileAppenderFactory, fileFactory, table.io(), TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

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

	public void abort() {
		if (partitionedFanoutWriter == null) {
			try {
				partitionedFanoutWriter.abort();
			} catch (IOException e) {
				log.error("Error while aborting Iceberg writer, error: {}", e.getMessage());
			}
		}
		
	}

	public void flush() {
		flush = true;
	}

}
