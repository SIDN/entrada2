package nl.sidn.entrada2.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

	@Value("#{${iceberg.parquet.max-file-size-mb:256} * 1024 * 1024}")
	private int maxFileSizeBytes;
	
	@Value("${iceberg.compression}")
	private String compressionAlgo;

	@Value("${iceberg.parquet.page-limit:20000}")
	private int parquetPageLimit;

	@Value("${iceberg.parquet.dictionary-max-bytes:20000}")
	private int parquetDictMaxBytes;

	@Value("${iceberg.table.bloomfilter:true}")
	private boolean enableBloomFilter;
	
	@Value("${iceberg.metadata.version.max:100}")
	private int metadataVersionMax;

	private long currentRecCount;

	@Autowired
	private Table table;
	
	private GenericRecord genericRecord;
	private GenericRecord genericRecordRdata;
	private PartitionedFanoutWriter<GenericRecord> partitionedFanoutWriter;

	private List<GenericRecord> pageRecords;

	@Autowired
	private LeaderQueue leaderQueue;

	@PostConstruct
	public void initialize() {
		this.genericRecord = GenericRecord.create(table.schema());
		this.genericRecordRdata = GenericRecord.create(table.schema().findType(49).asStructType());
		this.pageRecords = new ArrayList<GenericRecord>(parquetPageLimit);
	}

	private PartitionedFanoutWriter<GenericRecord> createWriter() throws IOException {
		
		currentRecCount = 0;

		// make sure to also use the spec when creating a GenericAppenderFactory
		// otherwise iceberg will not generate the partitioning metadata
		GenericAppenderFactory fileAppenderFactory = new GenericAppenderFactory(table.schema(), table.spec());

		fileAppenderFactory.set(TableProperties.PARQUET_COMPRESSION, compressionAlgo);
		fileAppenderFactory.set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
		fileAppenderFactory.set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, String.valueOf(metadataVersionMax));

		fileAppenderFactory.set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(maxFileSizeBytes));
		
		if (enableBloomFilter) {
			fileAppenderFactory.set(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + FieldEnum.dns_domainname.name(), "true");
		}

		// use small dict size otherwize the domainname column will use dictionary
		// ecoding and parquet
		// will only write a bloomfilter when dict encoding is NOT used for the column.
		// not using dict encoding for domainname will increase size of file but at
		// query time we can
		// potentially skip many files/rowgroups that do not contain records for a
		// domain
		fileAppenderFactory.set(TableProperties.PARQUET_DICT_SIZE_BYTES, "" + parquetDictMaxBytes);

		// keep rowgroups relatively small (20k) so they are easier to sort
		fileAppenderFactory.set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "" + parquetPageLimit);

		int partitionId = 1, taskId = 1;
		OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
				.format(FileFormat.PARQUET).build();

		// the WrappedPartitionedFanoutWriter will create multiple partitions if data in
		// the pcap is for multiple days
		return new WrappedPartitionedFanoutWriter(table, fileAppenderFactory, outputFileFactory);

	}

	public void clear() {
		pageRecords.clear();
		partitionedFanoutWriter = null;
	}

	public void write(GenericRecord record) {

		currentRecCount++;
		
		if(currentRecCount % 1000 == 0) {
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

		// do not write record to file until minimum number of records has been
		// collected.
		pageRecords.add(record);
		//new SortableGenericRecord(record, (String) record.get(FieldEnum.dns_domainname.ordinal())));
		if (currentRecCount % parquetPageLimit == 0) {
			// have min # of record, write batch to new page in file
			writePageBatch();
		}

	}

	private void writePageBatch() {
		// sort all rows, this will help compression also to better
		// compress the data
		
		log.info("writePageBatch: pageRecords = {}", pageRecords.size());

		pageRecords.sort(
			    Comparator.comparing(
			        r -> (String) r.get(FieldEnum.dns_domainname.ordinal()),
			        Comparator.nullsFirst(String::compareTo)
			    )
			);

		pageRecords.stream().forEach(r -> {
			try {
				partitionedFanoutWriter.write(r);
			} catch (Exception e) {
				log.error("Error writing row: {}", r, e);
			}
		});

		pageRecords.clear();
	}

	private List<DataFile> close() {
		
		if(log.isDebugEnabled()) {
			log.debug("Closing writer");
		}

		writePageBatch();

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
		log.info("Add new datafile to Iceberg table: " + dataFile.location());
		
		AppendFiles appendFiles = table.newAppend();
		appendFiles.appendFile(dataFile);
		appendFiles.commit();
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
		return genericRecord.copy();
	}
	
	public GenericRecord newRdataGenericRecord() {
		return genericRecordRdata.copy();
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

}
