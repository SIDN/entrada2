package nl.sidn.entrada2.service;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.data.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.load.DNSRowBuilder;
import nl.sidn.entrada2.load.DnsMetricValues;
import nl.sidn.entrada2.load.PacketJoiner;
import nl.sidn.entrada2.metric.HistoricalMetricManager;
import nl.sidn.entrada2.util.CompressionUtil;
import nl.sidn.entrada2.util.S3ObjectTagName;
import nl.sidn.entrada2.util.TimeUtil;
import nl.sidnlabs.pcap.PcapReader;

@Service
@Slf4j
public class WorkService {
	
	private static final int DECOMPRESS_STREAM_BUFFER = 256 * 1024;
	
	@Value("${entrada.nameserver.default-name}")
	private String defaultNsName;

	@Value("${entrada.nameserver.default-site}")
	private String defaultNsSite;
	
	@Value("${entrada.s3.pcap-in-dir}")
	private String pcapDirIn;
	
	@Value("${entrada.s3.pcap-done-dir}")
	private String pcapDirDone;
	
	@Value("${entrada.rdata.enabled:false}")
	private boolean rdataEnabled;
	
	@Value("${entrada.rdata.dnssec:false}")
	private boolean rdataDnsSecEnabled;
	
	@Value("${entrada.s3.pcap-delete:true}")
	private boolean pcapDelete;
	

	@Autowired
	private S3Service s3Service;
	@Autowired
	private PacketJoiner joiner;
	@Autowired
	private IcebergService icebergService;
	@Autowired
	private DNSRowBuilder rowBuilder;

	@Autowired(required = false)
	private HistoricalMetricManager metrics;

	@Value("#{${entrada.process.max-proc-time-secs:600}*1000}")
	private int stalledMillis;

	private long startOfWork;

	private MeterRegistry meterRegistry;

	private Counter okCounter;
	private Counter errorCounter;
	private boolean working;

	public WorkService(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	public void stop() {
		if (isStalled()) {
			// do nothing, process is hanging
			return;
		}
		// still working on pcap file, make sure to wait until file is completely
		// processed
		while (working) {
			TimeUtil.sleep(1000);
		}
		flush();
	}
	
	public void flush() {
		icebergService.commit();
	}

	public boolean process(String bucket, String key) {
		
		if(StringUtils.equalsIgnoreCase(pcapDirIn, key)) {
			// ingore input directory creation
			log.error("Ignore create of pcap prefix: {}", key);
			return false;
		}
		if(!CompressionUtil.isSupportedFormat(key)) {
			// ingore input directory creation
			log.error("Unsupported filetype: {}", key);
			return false;
		}
			
		Map<String, String> tags = new HashMap<String, String>();
		if(!s3Service.tags(bucket, key, tags)){
			// cannot get tags, retry later
			log.error("Cannot get tags for objec: {}", key);
			return false;
		}

		// check if file has been processed before, messages are delivered to single consumer
		// object will also get to here when code below updates the tags and causes new event to be sent
		// this does not cause a race condition if we check for the presence of the start_ts tag
		if(tags.keySet().contains(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value)) {
			log.info("s3 object has already been processed (tag {} is present), do not continue processing: {}", S3ObjectTagName.ENTRADA_PROCESS_TS_START.value, key);
			return true;
		}
		
		Integer tries = 1;
		if(tags.keySet().contains(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value)) {
			String value = tags.get(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value);
			if(NumberUtils.isCreatable(value)) {
				tries = NumberUtils.createInteger(tags.get(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value)) + 1;
			}
		}
				
		tags.put(S3ObjectTagName.ENTRADA_PROCESS_TS_START.value,
				LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
		tags.put(S3ObjectTagName.ENTRADA_OBJECT_TRIES.value, tries.toString());
		
		if(!s3Service.tag(bucket, key, tags)) {
			// could not mark the file as being processed, do not continue
			log.error("Claiming s3 object failed, do not continue processing: {}", key);
			return false;
		}

		String server = defaultNsName;
		if (tags.containsKey(S3ObjectTagName.ENTRADA_NS_SERVER.value)) {
			server = tags.get(S3ObjectTagName.ENTRADA_NS_SERVER.value);
		}
		
		String anycastSite = defaultNsSite;
		if (tags.containsKey(S3ObjectTagName.ENTRADA_NS_ANYCAST_SITE.value)) {
			anycastSite = tags.get(S3ObjectTagName.ENTRADA_NS_ANYCAST_SITE.value);
		}
		
		okCounter = Counter.builder("entrada_pcap-file").tags("server", server, "site", anycastSite, "status", "ok").register(meterRegistry);	
		errorCounter = Counter.builder("entrada_pcap-file").tags("server", server).tags("site", anycastSite).tag("status", "error").register(meterRegistry);
		
		Timer.Sample sample = Timer.start(meterRegistry);
		
		log.info("Start processing file: {}/{}, server: {}, site: {}", bucket, key, server, anycastSite);
		startOfWork = System.currentTimeMillis();
		long duration = 0;
		Optional<InputStream> ois = null;
		try {
			working = true;
			ois = s3Service.read(bucket, key);
			if (ois.isPresent()) {
				Optional<PcapReader> oreader = createReader(key, ois.get());
				if (oreader.isPresent()) {
					process_(oreader.get(), bucket, key, server, anycastSite);
				}
			}
			
			duration = System.currentTimeMillis() - startOfWork;
			okCounter.increment();	
			log.info("Done processing file: {}/{}, time: {}ms", bucket, key, duration);
		} catch (Exception e) {
			log.error("Error processing file: {}/{}", bucket, key, e);
			errorCounter.increment();
			return false;
		} finally {
			// startOfWork is also used for checking if pcap processing has stalled
			try {
				if(ois.isPresent()) {
					ois.get().close();
				}
			}catch (Exception e) {
				// ignore close error
				log.error("Error while closing inpustream for: {}/{}",  bucket, key);
			}
			startOfWork = 0;
			working = false;
			if(isMetricsEnabled()) {
				metrics.flush(server, anycastSite);
			}
		}
		
		//cleanup after successful processing of file
		if(isDeleteObject()) {
			// delete input file, do not copy to output location
			s3Service.delete(bucket, key);
		}else {
			// keep pcap file in s3, but mark as processed
			tags.put(S3ObjectTagName.ENTRADA_PROCESS_DURATION.value, String.valueOf(duration));
			tags.put(S3ObjectTagName.ENTRADA_PROCESS_TS_END.value,
					LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
			
			String tagKkey = key;
			if(StringUtils.isNotBlank(pcapDirDone)) {
				// move file to other directory prefix
				String file = StringUtils.substringAfterLast(key, "/");
				String newKey = pcapDirDone + "/" + file;
				s3Service.move(bucket, key, newKey );
				tagKkey = pcapDirDone + "/" + file;
			}
			
			s3Service.tag(bucket, tagKkey, tags);
		}
		
		sample.stop(meterRegistry.timer("entrada_pcap-timer", "server", server, "site", anycastSite));
	    
		return true;
	}
	
	private boolean isDeleteObject() {
		return pcapDelete;
	}

	private boolean isMetricsEnabled() {
		return metrics != null;
	}

	private void process_(PcapReader reader, String bucket, String key, String server, String anycastSite) {

		// make sure previously failed process has not left data behind
		icebergService.clear();

		reader.stream().forEach(p -> {
			
			joiner.join(p).forEach(rd -> {
				// for better performance, only create a newRdataGenericRecord when rdata output is enabled
				Pair<GenericRecord, DnsMetricValues> rowPair = rowBuilder.build(rd, server, anycastSite,
						icebergService.newGenericRecord(), rdataEnabled? icebergService.newRdataGenericRecord(): null);

				// save all records from file in memory and only commit (write to file)
				// when file has been read succesfully, to prevent reading same packets again from file when doing retry.
				icebergService.write(rowPair.getKey());

				// update metrics
				if(isMetricsEnabled()) {
					metrics.update(rowPair.getValue());
				}
			});
		});

		if (log.isDebugEnabled()) {
			log.debug("Extracted all data from file, now clear joiner cache");
		}

		// clear joiner cache, unmatched queries will get rcode -1
		joiner.clearCache().forEach(rd -> {
			
			Pair<GenericRecord, DnsMetricValues> rowPair = rowBuilder.build(rd, server, anycastSite,
					icebergService.newGenericRecord(), rdataEnabled? icebergService.newRdataGenericRecord(): null);
			
			icebergService.write(rowPair.getKey());

			// update metrics
			if(isMetricsEnabled()) {
				metrics.update(rowPair.getValue());
			}

		});

		if (log.isDebugEnabled()) {
			log.debug("Close Iceberg writer");
		}

		icebergService.commit();

		if (log.isDebugEnabled()) {
			log.debug("Close pcap reader");
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

	private Optional<PcapReader> createReader(String file, InputStream is) {

		try {
			InputStream decompressor = CompressionUtil.getDecompressorStreamWrapper(is, DECOMPRESS_STREAM_BUFFER, file);
			return Optional.of(new PcapReader(new DataInputStream(decompressor), null, true, !rdataEnabled));
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
