package nl.sidn.entrada2.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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

	@Value("${iceberg.parquet.sort-column:}")
	private String sortColumn;

	// number of records per sorted chunk before spilling to disk (only used when sort-column is set)
	@Value("${iceberg.parquet.sort-chunk-size:1000000}")
	private int sortChunkSize;

	// I/O buffer size in bytes for spill file reads and writes (only used when sort-column is set)
	@Value("#{${iceberg.parquet.sort-spill-buffer-mb:2} * 1024 * 1024}")
	private int spillBufferSize;

	// cached field position for sort column, resolved at startup (-1 = sorting disabled)
	private int sortFieldPos = -1;

	// number of recs written to current file
	private long currentRecCount;

	@Autowired
	private Table table;
	
	// Store schemas instead of template records to avoid copy overhead
	private StructType recordSchema;
	private StructType rdataSchema;
	private List<GenericRecord> recordBuffer = new ArrayList<>();
	private PartitionedFanoutWriter<GenericRecord> partitionedFanoutWriter;
	// temp files holding sorted chunks for external merge sort
	private List<File> spillFiles = new ArrayList<>();

	private int writeErrors;

	private boolean flush = false;

	@Autowired
	private LeaderQueue leaderQueue;

	// cached field lists to avoid repeated list creation in hot write/read paths
	private List<org.apache.iceberg.types.Types.NestedField> recordFields;
	// cached string representations of config values used in createWriter()
	private String bloomFilterMaxBytesStr;
	private String dictMaxBytesStr;
	// cached sort comparator to avoid re-creating on every spillSortedChunk() call
	private Comparator<GenericRecord> sortComparator;
	// reusable read buffer for string deserialization (expanded on demand)
	private byte[] stringReadBuffer = new byte[256];
	// String intern cache: avoids creating new String objects for repeated values
	// (domain names, IPs, country codes, ASN orgs repeat millions of times in DNS data).
	// Open-addressing with FNV-1a hash; slot includes field index so high-cardinality
	// fields (dns_domainname, ip_src) cannot evict entries from low-cardinality fields
	// (server, server_location, ip_geo_country) that would otherwise be near-perfect hits.
	// False-positive collisions (~1/4B) are harmless: just one extra String allocation.
	private static final int STRING_CACHE_MASK = (1 << 16) - 1; // 64K slots
	private final String[] stringCache = new String[STRING_CACHE_MASK + 1];
	private final int[] stringCacheHash = new int[STRING_CACHE_MASK + 1];
	// set by readRow() before each field so internString() can isolate slots per field
	private int currentReadFieldIndex = 0;
	// intern cache stats, reset per merge run
	private long cacheHits;
	private long cacheMisses;

	@PostConstruct
	public void initialize() {
		this.recordSchema = table.schema().asStruct();
		this.rdataSchema = ((org.apache.iceberg.types.Types.ListType) table.schema().findField("dns_rdata").type())
				.elementType().asStructType();
		this.recordFields = recordSchema.fields();
		this.bloomFilterMaxBytesStr = String.valueOf(parquetBloomFilterMaxBytes);
		this.dictMaxBytesStr = String.valueOf(parquetDictMaxBytes);
		if (!StringUtils.isBlank(sortColumn)) {
			List<org.apache.iceberg.types.Types.NestedField> columns = table.schema().columns();
			for (int i = 0; i < columns.size(); i++) {
				if (sortColumn.equals(columns.get(i).name())) {
					sortFieldPos = i;
					break;
				}
			}
			if (sortFieldPos < 0) {
				log.warn("Sort column '{}' not found in schema, sorting disabled", sortColumn);
			} else {
				log.info("Sort column '{}' at position {}, row group size: {}", sortColumn, sortFieldPos, sortChunkSize);
				//noinspection unchecked
				sortComparator = Comparator.comparing(
						r -> (Comparable<Object>) r.get(sortFieldPos),
						Comparator.nullsLast(Comparator.naturalOrder()));
			}
		}
	}

	private PartitionedFanoutWriter<GenericRecord> createWriter() throws IOException {
		
		currentRecCount = 0;

		// make sure to also use the spec when creating a GenericAppenderFactory
		// otherwise iceberg will not generate the partitioning metadata
		GenericAppenderFactory fileAppenderFactory = new GenericAppenderFactory(table.schema(), table.spec());

		fileAppenderFactory.set(TableProperties.PARQUET_COMPRESSION, compressionAlgo);
		
		if (enableBloomFilter) {
			fileAppenderFactory.set(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + FieldEnum.dns_domainname.name(), "true");
			fileAppenderFactory.set(TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES, bloomFilterMaxBytesStr);
		}

		// use small dict size otherwize the domainname column will use dictionary
		// ecoding and parquet
		// will only write a bloomfilter when dict encoding is NOT used for the column.
		// not using dict encoding for domainname will increase size of file but at
		// query time we can
		// potentially skip many files/rowgroups that do not contain records for a
		// domain
		fileAppenderFactory.set(TableProperties.PARQUET_DICT_SIZE_BYTES, dictMaxBytesStr);

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
		recordBuffer = new ArrayList<>(sortChunkSize > 0 ? sortChunkSize : 16);
		partitionedFanoutWriter = null;
		deleteSpillFiles();
	}

	public void write(GenericRecord record) {

		currentRecCount++;
		
		if(currentRecCount % 100000 == 0) {
			log.info("Processed {} rows", currentRecCount);
		}

		if (sortFieldPos < 0) {
			// no sorting: write directly to writer
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
				writeErrors++;
				if(log.isDebugEnabled()) {
					log.debug("Error writing row: {}", record, e);
				}
			}
		} else {
			// sorting: buffer until chunk is full, then spill to disk
			recordBuffer.add(record);
			if (recordBuffer.size() >= sortChunkSize) {
				spillSortedChunk();
			}
		}
	}

	/**
	 * Sort all buffered records in memory and write directly to a single Parquet writer.
	 * Used when parquetMinRecords <= 1_000_000 (dataset fits comfortably in heap).
	 */
	private List<DataFile> sortAndWriteInMemory() {
		if (recordBuffer.isEmpty()) {
			return Collections.emptyList();
		}
		log.info("In-memory sort of {} records on column '{}'" , recordBuffer.size(), sortColumn);
		recordBuffer.sort(sortComparator);
		try {
			PartitionedFanoutWriter<GenericRecord> writer = createWriter();
			for (GenericRecord record : recordBuffer) {
				try {
					writer.write(record);
				} catch (IOException e) {
					writeErrors++;
					if (log.isDebugEnabled()) log.debug("Error writing row: {}", record, e);
				}
			}
			List<DataFile> files = Arrays.stream(writer.dataFiles()).toList();
			log.info("In-memory sort complete, result: {} data files", files.size());
			return files;
		} catch (Exception e) {
			log.error("In-memory sort write failed", e);
			return Collections.emptyList();
		}
	}

	/**
	 * Sort the current buffer and spill it to a temp file using fast binary encoding.
	 * Memory is freed after spill; the file is merged later in mergeAndWrite().
	 */
	private void spillSortedChunk() {
		log.info("Spilling record chunk to disk" );

		if (recordBuffer.isEmpty()) {
			return;
		}
		List<GenericRecord> chunk = recordBuffer;
		recordBuffer = new ArrayList<>(sortChunkSize);
		chunk.sort(sortComparator);

		try {
			File tempFile = File.createTempFile("entrada-sort-", ".tmp");
			tempFile.deleteOnExit();
			try (DataOutputStream dos = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(tempFile), spillBufferSize))) {
				dos.writeInt(chunk.size());
				for (GenericRecord record : chunk) {
					writeRow(dos, record);
				}
			}
			spillFiles.add(tempFile);
			log.info("Spilled sorted chunk of {} records to {}", chunk.size(), tempFile.getName());
		} catch (IOException e) {
			throw new RuntimeException("Cannot spill sorted chunk to disk", e);
		}
	}

	/**
	 * K-way merge all spill files using a PriorityQueue, streaming merged records
	 * directly into a single Parquet writer. Memory use is O(number of chunks).
	 */
	private List<DataFile> mergeAndWrite() {
		if (spillFiles.isEmpty()) {
			return Collections.emptyList();
		}
		log.info("Merging {} sorted chunks", spillFiles.size());
		cacheHits = 0;
		cacheMisses = 0;
		List<SpillChunkReader> readers = new ArrayList<>();
		try {
			PriorityQueue<SpillChunkReader> pq = new PriorityQueue<>();
			for (File f : spillFiles) {
				SpillChunkReader reader = new SpillChunkReader(f);
				readers.add(reader);
				if (reader.hasNext()) {
					pq.add(reader);
				}
			}
			PartitionedFanoutWriter<GenericRecord> writer = createWriter();
			while (!pq.isEmpty()) {
				SpillChunkReader reader = pq.poll();
				try {
					// reader.current() is a reusable GenericRecord owned by the reader;
					// Parquet encodes it synchronously so it's safe to reuse immediately after
					writer.write(reader.current());
				} catch (IOException e) {
					writeErrors++;
					if (log.isDebugEnabled()) log.debug("Error writing row", e);
				}
				reader.advance();
				if (reader.hasNext()) {
					pq.add(reader);
				}
			}
			List<DataFile> files = Arrays.stream(writer.dataFiles()).toList();
			long total = cacheHits + cacheMisses;
			log.info("Merge complete: string cache hits={} misses={} hit-rate={}%",
					cacheHits, cacheMisses,
					total > 0 ? String.format("%.1f", 100.0 * cacheHits / total) : "n/a");
			return files;
		} catch (Exception e) {
			log.error("Merge failed", e);
			return Collections.emptyList();
		} finally {
			for (SpillChunkReader r : readers) {
				try { r.close(); } catch (Exception ignored) {}
			}
			spillFiles.clear();
		}
	}

	// --- Binary I/O helpers for spill files (schema-aware, no Java serialization overhead) ---

	private void writeRow(DataOutputStream dos, GenericRecord record) throws IOException {
		for (int i = 0; i < recordFields.size(); i++) {
			writeValue(dos, record.get(i), recordFields.get(i).type());
		}
	}

	private void writeValue(DataOutputStream dos, Object value, org.apache.iceberg.types.Type type) throws IOException {
		if (value == null) {
			dos.writeByte(0);
			return;
		}
		dos.writeByte(1);
		switch (type.typeId()) {
			case STRING -> { byte[] b = value.toString().getBytes(StandardCharsets.UTF_8); dos.writeInt(b.length); dos.write(b); }
			case INTEGER -> dos.writeInt((Integer) value);
			case LONG -> dos.writeLong((Long) value);
			case BOOLEAN -> dos.writeBoolean((Boolean) value);
			case FLOAT -> dos.writeFloat((Float) value);
			case DOUBLE -> dos.writeDouble((Double) value);
			case DATE -> dos.writeInt((Integer) value);
			case TIME -> {
				// Iceberg TIME: Long (micros since midnight) or LocalTime
				long micros = value instanceof Long l ? l : ((LocalTime) value).toNanoOfDay() / 1000L;
				dos.writeLong(micros);
			}
			case TIMESTAMP -> {
				// Iceberg TIMESTAMP: Long (micros since epoch), LocalDateTime, or OffsetDateTime
				long micros;
				if (value instanceof Long l) {
					micros = l;
				} else if (value instanceof LocalDateTime ldt) {
					micros = ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000L;
				} else {
					OffsetDateTime odt = (OffsetDateTime) value;
					micros = odt.toEpochSecond() * 1_000_000L + odt.getNano() / 1000L;
				}
				dos.writeByte(value instanceof OffsetDateTime ? 2 : value instanceof LocalDateTime ? 1 : 0);
				dos.writeLong(micros);
			}
			case LIST -> {
				List<?> list = (List<?>) value;
				org.apache.iceberg.types.Type elemType = ((org.apache.iceberg.types.Types.ListType) type).elementType();
				dos.writeInt(list.size());
				for (Object item : list) writeValue(dos, item, elemType);
			}
			case STRUCT -> {
				GenericRecord nested = (GenericRecord) value;
				List<org.apache.iceberg.types.Types.NestedField> fields = ((StructType) type).fields();
				for (int i = 0; i < fields.size(); i++) writeValue(dos, nested.get(i), fields.get(i).type());
			}
			default -> throw new IOException("Unsupported Iceberg type for spill: " + type.typeId());
		}
	}

	private void readRow(DataInputStream dis, GenericRecord record) throws IOException {
		for (int i = 0; i < recordFields.size(); i++) {
			currentReadFieldIndex = i;
			record.set(i, readValue(dis, recordFields.get(i).type()));
		}
	}

	private Object readValue(DataInputStream dis, org.apache.iceberg.types.Type type) throws IOException {
		if (dis.readByte() == 0) return null;
		return switch (type.typeId()) {
			case STRING -> {
				int len = dis.readInt();
				if (len > stringReadBuffer.length) stringReadBuffer = new byte[len];
				dis.readFully(stringReadBuffer, 0, len);
				yield internString(len);
			}
			case INTEGER -> dis.readInt();
			case LONG -> dis.readLong();
			case BOOLEAN -> dis.readBoolean();
			case FLOAT -> dis.readFloat();
			case DOUBLE -> dis.readDouble();
			case DATE -> dis.readInt();
			case TIME -> LocalTime.ofNanoOfDay(dis.readLong() * 1000L);
			case TIMESTAMP -> {
				int subtype = dis.readByte();
				long micros = dis.readLong();
				if (subtype == 1) {
					yield LocalDateTime.ofEpochSecond(micros / 1_000_000L, (int)((micros % 1_000_000L) * 1000L), ZoneOffset.UTC);
				} else if (subtype == 2) {
					yield OffsetDateTime.of(LocalDateTime.ofEpochSecond(micros / 1_000_000L, (int)((micros % 1_000_000L) * 1000L), ZoneOffset.UTC), ZoneOffset.UTC);
				} else {
					yield micros;
				}
			}
			case LIST -> {
				int size = dis.readInt();
				org.apache.iceberg.types.Type elemType = ((org.apache.iceberg.types.Types.ListType) type).elementType();
				List<Object> list = new ArrayList<>(size);
				for (int i = 0; i < size; i++) list.add(readValue(dis, elemType));
				yield list;
			}
			case STRUCT -> {
				StructType st = (StructType) type;
				GenericRecord nested = GenericRecord.create(st);
				List<org.apache.iceberg.types.Types.NestedField> fields = st.fields();
				for (int i = 0; i < fields.size(); i++) nested.set(i, readValue(dis, fields.get(i).type()));
				yield nested;
			}
			default -> throw new IOException("Unsupported Iceberg type for spill: " + type.typeId());
		};
	}

	/**
	 * FNV-1a string intern cache: returns a cached String instance for byte sequences
	 * that were seen before, avoiding new String allocation for repeated values.
	 * Uses stringReadBuffer[0..len-1] as input (no extra allocation on cache hit).
	 */
	private String internString(int len) {
		int hash = 0x811c9dc5;
		for (int i = 0; i < len; i++) {
			hash ^= stringReadBuffer[i] & 0xFF;
			hash *= 0x01000193;
		}
		// Mix field index into slot: each field maps to a separate cache region,
		// so high-cardinality fields (domainname, ip_src) cannot evict entries
		// for low-cardinality fields (server, country) that repeat constantly.
		int slot = (hash ^ (currentReadFieldIndex * 0x9e3779b9)) & STRING_CACHE_MASK;
		if (stringCacheHash[slot] == hash && stringCache[slot] != null) {
			cacheHits++;
			return stringCache[slot];
		}
		cacheMisses++;
		String s = new String(stringReadBuffer, 0, len, StandardCharsets.UTF_8);
		stringCache[slot] = s;
		stringCacheHash[slot] = hash;
		return s;
	}

	private void deleteSpillFiles() {
		for (File f : spillFiles) {
			try { f.delete(); } catch (Exception ignored) {}
		}
		spillFiles.clear();
	}

	private List<DataFile> close() {
		
		if(log.isDebugEnabled()) {
			log.debug("Closing writer");
		}

		log.info("Close Iceberg writer, records: {} errors: {}", currentRecCount, writeErrors);

		if (sortFieldPos >= 0) {
			if (spillFiles.isEmpty()) {
				// no spills yet: all records fit in buffer, sort in memory (fast path)
				return sortAndWriteInMemory();
			} else {
				// spills already occurred: spill remainder and k-way merge (memory-safe path)
				spillSortedChunk();
				return mergeAndWrite();
			}
		}

		if (partitionedFanoutWriter != null) {
			try {
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
		recordBuffer = new ArrayList<>();
		deleteSpillFiles();
		if (partitionedFanoutWriter != null) {
			try {
				partitionedFanoutWriter.abort();
			} catch (IOException e) {
				log.error("Error while aborting Iceberg writer, error: {}", e.getMessage());
			}
			partitionedFanoutWriter = null;
		}
	}

	public void flush() {
		flush = true;
	}

	/**
	 * Reads sorted rows from a spill file one at a time using fast binary decoding.
	 * Each reader owns a reusable GenericRecord; no Object[] allocation per row.
	 */
	private class SpillChunkReader implements Comparable<SpillChunkReader>, java.io.Closeable {

		private final DataInputStream dis;
		private final File file;
		private final GenericRecord record;
		private boolean hasNext;
		private int remaining;

		SpillChunkReader(File file) throws IOException {
			this.file = file;
			this.dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file), spillBufferSize));
			this.record = GenericRecord.create(recordSchema);
			this.remaining = dis.readInt();
			advance();
		}

		boolean hasNext() {
			return hasNext;
		}

		/** Returns the current record. Valid until the next advance() call. */
		GenericRecord current() {
			return record;
		}

		void advance() {
			if (remaining > 0) {
				try {
					readRow(dis, record);
					remaining--;
					hasNext = true;
				} catch (Exception e) {
					log.error("Error reading spill file {}", file.getName(), e);
					hasNext = false;
				}
			} else {
				hasNext = false;
			}
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		@Override
		public int compareTo(SpillChunkReader other) {
			Comparable a = hasNext ? (Comparable) record.get(sortFieldPos) : null;
			Comparable b = other.hasNext ? (Comparable) other.record.get(sortFieldPos) : null;
			if (a == null && b == null) return 0;
			if (a == null) return 1;  // nulls last
			if (b == null) return -1;
			return a.compareTo(b);
		}

		@Override
		public void close() throws IOException {
			try { dis.close(); } finally { file.delete(); }
		}
	}

}
