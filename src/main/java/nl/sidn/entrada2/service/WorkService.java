package nl.sidn.entrada2.service;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.data.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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

	@Value("${entrada.inputstream.buffer:64}")
	private int bufferSizeConfig;

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

	@Value("#{${entrada.process.stalled:10}*60*1000}")
	private int stalledMillis;

	private long startOfWork;

	private MeterRegistry meterRegistry;

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
		icebergService.commit();
	}

	public void process(String bucket, String key) {

		Map<String, String> tags = s3Service.tags(bucket, key);
		String server = "unknown";
		if (tags.containsKey(S3ObjectTagName.ENTRADA_NS_SERVER.value)) {
			server = tags.get(S3ObjectTagName.ENTRADA_NS_SERVER.value);
		}
		String anycastSite = "unknown";
		if (tags.containsKey(S3ObjectTagName.ENTRADA_NS_ANYCAST_SITE.value)) {
			anycastSite = tags.get(S3ObjectTagName.ENTRADA_NS_ANYCAST_SITE.value);
		}

		log.info("Start processing file: {}/{}, server: {}, site: {}", bucket, key, server, anycastSite);
		startOfWork = System.currentTimeMillis();
		try {
			working = true;
			process_(bucket, key, server, anycastSite);
			long duration = System.currentTimeMillis() - startOfWork;

			Counter.builder("pcap.processed").tags("server", server).tags("location", anycastSite)
					.register(meterRegistry).increment();

			tags.put(S3ObjectTagName.ENTRADA_PROCESSED_OK.value, "yes");
			tags.put(S3ObjectTagName.ENTRADA_PROCESS_DURATION.value, String.valueOf(duration));
			tags.put(S3ObjectTagName.ENTRADA_PROCESS_TS.value,
					LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));

			log.info("Finished processing file: {}/{}, time: {}ms", bucket, key, duration);
		} catch (Exception e) {

			log.error("Error processing file: {}/{}", bucket, key, e);
			tags.put(S3ObjectTagName.ENTRADA_PROCESSED_OK.value, "no");

			Counter.builder("pcap.error").tags("server", server).tags("location", anycastSite).register(meterRegistry)
					.increment();

		} finally {
			// startOfWork is also use to check for stalled processing, reset after done
			// with file
			startOfWork = 0;
			working = false;
			if(isMetricsEnabled()) {
				metrics.flush(server);
			}
			meterRegistry.clear();
		}

		s3Service.tag(bucket, key, tags);
	}

	private boolean isMetricsEnabled() {
		return metrics != null;
	}

	private void process_(String bucket, String key, String server, String anycastSite) {

		Optional<InputStream> ois = s3Service.read(bucket, key);
		if (ois.isPresent()) {
			Optional<PcapReader> oreader = createReader(key, ois.get());
			if (oreader.isPresent()) {

				oreader.get().stream().forEach(p -> {

					joiner.join(p).forEach(rd -> {
						Pair<GenericRecord, DnsMetricValues> rowPair = rowBuilder.build(rd, server, anycastSite,
								icebergService.newGenericRecord());
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
							icebergService.newGenericRecord());
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

				oreader.get().close();
			}

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
			InputStream decompressor = CompressionUtil.getDecompressorStreamWrapper(is, bufferSizeConfig * 1024, file);
			return Optional.of(new PcapReader(new DataInputStream(decompressor), null, true, file, false));
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
