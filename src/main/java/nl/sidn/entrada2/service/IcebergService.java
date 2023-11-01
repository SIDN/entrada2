package nl.sidn.entrada2.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
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

@Service
@Slf4j
public class IcebergService {

	@Value("#{${entrada.parquet.file.max-size:256} * 1024 * 1024}")
	private int maxFileSizeMegabyte;

	@Value("${iceberg.compression}")
	private String compressionAlgo;

	@Value("${iceberg.table.sorted:true}")
	private boolean enableSorting;

	@Value("${iceberg.parquet.page-limit:20000}")
	private int parquetPageLimit;

	@Value("${iceberg.parquet.dictionary-max-bytes:20000}")
	private int parquetDictMaxBytes;

	@Value("${iceberg.table.bloomfilter:true}")
	private boolean enableBloomFilter;

	@Autowired
	private Table table;

	private GenericRecord genericRecord;
	private WrappedPartitionedFanoutWriter partitionedFanoutWriter;

	private BlockingQueue<DataFile> datafileQueue = new LinkedBlockingQueue<>();

	private List<SortableGenericRecord> records;

	@PostConstruct
	public void initialize() {
		this.genericRecord = GenericRecord.create(table.schema());
		this.records = new ArrayList<SortableGenericRecord>(parquetPageLimit);
	}

	private void createWriter() throws IOException {

		// make sure to also use the spec when creating a GenericAppenderFactory
		// otherwise
		// iceberg will not generate the partitioning metadata
		GenericAppenderFactory fileAppenderFactory = new GenericAppenderFactory(table.schema(), table.spec());

		// user gzip to create smaller files to limit athena io cost
		fileAppenderFactory.set("write.parquet.compression-codec", compressionAlgo);
		fileAppenderFactory.set("write.metadata.delete-after-commit.enabled", "true");
		fileAppenderFactory.set("write.metadata.previous-versions-max", "50");

		if (enableBloomFilter) {
			fileAppenderFactory.set(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "domainname", "true");
		}
		
		// use small dict size otherwize the domainname column will use dictionary ecoding and parquet
		// will only write a bloomfilter when dict encoding is NOT used for the column.
		// not using dict encoding for domainname will increase size of file but at query time we can 
		// potentially skip many files/rowgroups that do not contain records for a domain
		fileAppenderFactory.set(TableProperties.PARQUET_DICT_SIZE_BYTES, "" + parquetDictMaxBytes);

		// keep rowgroups relatively small (20k) so they are easier to sort
		fileAppenderFactory.set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "" + parquetPageLimit);

		int partitionId = 1, taskId = 1;
		OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
				.format(FileFormat.PARQUET).build();

		// the WrappedPartitionedFanoutWriter will create multiple partitions if data in
		// the pcap is for multiple days
		partitionedFanoutWriter = new WrappedPartitionedFanoutWriter(table, fileAppenderFactory, outputFileFactory);

	}

	public void write(GenericRecord record) {

		if (partitionedFanoutWriter == null) {

			try {
				createWriter();
			} catch (IOException e) {
				throw new RuntimeException("Cannot create writer", e);
			}
		}

		if (enableSorting) {

			records.add(new SortableGenericRecord(record, (String) record.get(FieldEnum.domainname.ordinal())));
			if (records.size() == parquetPageLimit) {
				writeBatch();
			}
		} else {
			try {
				partitionedFanoutWriter.write(record);
			} catch (Exception e) {
				log.error("Error writing row: {}", record, e);
			}
		}

//			Collections.sort(records);
//
//		try {
//			partitionedFanoutWriter.write(record);
//		} catch (Exception e) {
//			log.error("Error writing row: {}", record, e);
//		}
//
//			records.clear();
//		}
	}

	private void writeBatch() {
		// sort all rows in a datapsge, this will help compression algo to better compress the data
		Collections.sort(records);

		records.stream().forEach(r -> {
			try {
				partitionedFanoutWriter.write(r.getRec());
			} catch (Exception e) {
				log.error("Error writing row: {}", r, e);
			}
		});

		records.clear();
	}

	public List<DataFile> close() {

		if (enableSorting) {
			writeBatch();
		}

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
			log.error("Adding datafile to commit queue failed", e);
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
			super(table.spec(), FileFormat.PARQUET, appenderFactory, fileFactory, table.io(), maxFileSizeMegabyte);

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

//  public class GenericAppenderFactory2 extends GenericAppenderFactory{
//	  
//	  private final Schema schema;
//	  private final PartitionSpec spec;
//
//	public GenericAppenderFactory2(Schema schema, PartitionSpec spec) {
//		super(schema, spec);
//		this.schema = schema;
//		this.spec = spec;
//	}
//	  
//	
//	  @Override
//	  public org.apache.iceberg.io.DataWriter<Record> newDataWriter(
//	      EncryptedOutputFile file, FileFormat format, StructLike partition) {
//		  
//		  SortOrder so =  SortOrder.builderFor(schema).asc("domainname").build();
//		  
//	    return new org.apache.iceberg.io.DataWriter<>(
//	        newAppender(file.encryptingOutputFile(), format),
//	        format,
//	        file.encryptingOutputFile().location(),
//	        spec,
//	        partition,
//	        file.keyMetadata(),
//	        so);
//	  }
//  }

	@lombok.Value
	public class SortableGenericRecord implements Comparable<SortableGenericRecord> {
		private GenericRecord rec;
		private String domain;

		@Override
		public int compareTo(SortableGenericRecord o) {
			// TODO Auto-generated method stub
			return StringUtils.compare(domain, o.getDomain());
		}
	}

}
