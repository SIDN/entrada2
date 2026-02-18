/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidn.entrada2.metric;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.write.WritePrecision;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.load.DnsMetricValues;

/**
 * MetricManager is used to recreate metrics for DNS packets found in PCAP
 * files. The timestamp of a packets in the PCAP file is used when generating
 * the metrics and NOT the timestamp at the point in time when the packet was
 * read from the PCAP.
 * 
 * Thread-safe singleton that aggregates metrics in memory and periodically
 * flushes them to InfluxDB asynchronously.
 */
@Slf4j
@Component
@ConditionalOnExpression(
	    "T(org.apache.commons.lang3.StringUtils).isNotEmpty('${management.influx.metrics.export.uri}')"
	)
public class HistoricalMetricManager {

	public static final String METRIC_SEPERATOR = ":";
	// dns stats
	public static final String METRIC_IMPORT_DNS_QUERY_COUNT = "entrada_dns_request";
	public static final String METRIC_IMPORT_DNS_RESPONSE_COUNT = "entrada_dns_response";

	public static final String METRIC_IMPORT_DNS_QTYPE = "entrada_dns_qtype";
	public static final String METRIC_IMPORT_DNS_RCODE = "entrada_dns_rcode";
	public static final String METRIC_IMPORT_DNS_OPCODE = "entrada_dns_opcode";
	public static final String METRIC_IMPORT_DNS_PROC_TIME = "entrada_dns_proc-time";

	// layer 4 stats
	public static final String METRIC_IMPORT_NETWORK_PROT = "entrada_network_protocol";

	public static final String METRIC_IMPORT_IP_COUNT = "entrada_ip_version";
	public static final String METRIC_IMPORT_COUNTRY_COUNT = "entrada_geo_country";

	public static final String METRIC_IMPORT_TCP_HANDSHAKE_RTT = "entrada_tcp_rtt";
	
	// Reusable tag maps to avoid creating new maps for every metric
	public static final Map<String, String> IP_V4_TAG_MAP = Map.of("version", "4");
	public static final Map<String, String> IP_V6_TAG_MAP = Map.of("version", "6");
	
	public static final Map<String, String> NETWORK_UDP_TAG_MAP = Map.of("protocol", "UDP");
	public static final Map<String, String> NETWORK_TCP_TAG_MAP = Map.of("protocol", "TCP");

	private static final Random RND = new Random();
	private static final Map<String, String> EMPTY_MAP = Map.of();

	// Thread-safe cache: name -> time -> value
	private final Map<String, Map<Instant, Metric>> metricCache = new ConcurrentHashMap<>();

	@Value("${entrada.metrics.bin-size-secs:10}")
	private int binSizeSeconds;

	@Autowired(required = false)
	private InfluxDBClient influxClient;

	public void update(DnsMetricValues dmv, String server, String anycastSite) {

		if (dmv == null) {
			// do nothing
			return;
		}

		// Round timestamp to configured bin size (e.g., 10 seconds)
		long epochSeconds = dmv.time / 1000;
		long binnedSeconds = (epochSeconds / binSizeSeconds) * binSizeSeconds;
		Instant time = Instant.ofEpochSecond(binnedSeconds);

		if (dmv.dnsQuery) {
			update(METRIC_IMPORT_DNS_QUERY_COUNT, time, 1, true, EMPTY_MAP, server, anycastSite);
		}

		if (dmv.dnsResponse) {
			update(METRIC_IMPORT_DNS_RESPONSE_COUNT, time, 1, true, EMPTY_MAP, server, anycastSite);
		}

		if(dmv.dnsQtype != null) {
			String qtypeName = dmv.dnsQtype.name();
			update(METRIC_IMPORT_DNS_QTYPE + METRIC_SEPERATOR + qtypeName, time, 1, true,
					Map.of("qtype", qtypeName), server, anycastSite);
		}
		
		update(METRIC_IMPORT_DNS_RCODE + METRIC_SEPERATOR + dmv.dnsRcode, time, 1, true,
				Map.of("rcode", String.valueOf(dmv.dnsRcode)), server, anycastSite);
		update(METRIC_IMPORT_DNS_OPCODE + METRIC_SEPERATOR + dmv.dnsOpcode, time, 1, true,
				Map.of("opcode", String.valueOf(dmv.dnsOpcode)), server, anycastSite);

		if (StringUtils.isNotEmpty(dmv.country)) {
			update(METRIC_IMPORT_COUNTRY_COUNT + METRIC_SEPERATOR + dmv.country, time, 1, true,
					Map.of("name", dmv.country), server, anycastSite);
		}

		if (dmv.ProtocolUdp) {
			update(METRIC_IMPORT_NETWORK_PROT + METRIC_SEPERATOR + "udp", time, 1, true, NETWORK_UDP_TAG_MAP, server, anycastSite);
		} else {
			update(METRIC_IMPORT_NETWORK_PROT + METRIC_SEPERATOR + "tcp", time, 1, true, NETWORK_TCP_TAG_MAP, server, anycastSite);
			if (dmv.hasTcpHandshake()) {
				update(METRIC_IMPORT_TCP_HANDSHAKE_RTT, time, dmv.tcpHandshake, false, EMPTY_MAP, server, anycastSite);
			}
		}

		if (dmv.ipV4) {
			update(METRIC_IMPORT_IP_COUNT + METRIC_SEPERATOR + "4", time, 1, true, IP_V4_TAG_MAP, server, anycastSite);
		} else {
			update(METRIC_IMPORT_IP_COUNT + METRIC_SEPERATOR + "6", time, 1, true, IP_V6_TAG_MAP, server, anycastSite);
		}
		
		
		update(METRIC_IMPORT_DNS_PROC_TIME, time, (int)dmv.procTime, false, EMPTY_MAP, server, anycastSite);
		
	}

	private void update(String name, Instant time, int value, boolean counter, Map<String, String> tags, String server, String anycastSite) {
		// Use computeIfAbsent for thread-safe lazy initialization
		Map<Instant, Metric> metricValues = metricCache.computeIfAbsent(name, k -> new ConcurrentHashMap<>());
		
		// Use computeIfAbsent or merge for atomic update
		metricValues.compute(time, (k, existingMetric) -> {
			if (existingMetric == null) {
				return createMetric(value, counter, tags, server, anycastSite);
			} else {
				existingMetric.update(value);
				return existingMetric;
			}
		});
	}

	public static Metric createMetric(int value, boolean counter, Map<String, String> labels, String server, String anycastSite) {
		if (counter) {
			return new SumMetric(value, labels, server, anycastSite);
		}
		return new AvgMetric(value, labels, server, anycastSite);
	}

	/**
	 * Flush metrics to InfluxDB asynchronously.
	 * This method runs on a separate thread and will not block the caller.
	 * Server and anycast site information is already stored in each metric.
	 */
	@Async("metricsTaskExecutor")
	public void flush() {
		if (log.isDebugEnabled() && metricCache.isEmpty()) {
			log.debug("No metrics to flush");
			return;
		}
		
		log.info("Start async flush of metrics (entries: {})", metricCache.size());
		
		// Create snapshot and clear cache atomically
		Map<String, Map<Instant, Metric>> snapshot = new HashMap<>(metricCache);
		metricCache.clear();
		
		List<Point> points = new ArrayList<>();
		
		try {
			for(Entry<String, Map<Instant, Metric>> entry : snapshot.entrySet()) {
				entry.getValue().entrySet().forEach(m -> createPoints(points, m, entry.getKey()));
			}
			log.info("Writing {} metric points to InfluxDB", points.size());
			if (influxClient != null && !points.isEmpty()) {
				influxClient.writePoints(points);
				log.info("Successfully wrote {} points to InfluxDB", points.size());
			}
		} catch (Exception e) {
			// cannot connect to influxdb?
			log.error("Error sending {} metrics to InfluxDB", points.size(), e);
		}
		
		log.debug("Done flushing metrics");
	}
	
	/**
	 * Automatically flush metrics every 60 seconds.
	 * This is a fallback to ensure metrics are eventually written even if
	 * flush() is not called explicitly.
	 */
	@Scheduled(fixedDelayString = "${entrada.metrics.flush.interval:60000}")
	public void autoFlush() {
		if (!metricCache.isEmpty()) {
			log.info("Auto-flushing metrics (scheduled task)");
			flush();
		}
	}

	private void createPoints(List<Point> points, Entry<Instant, Metric> entry, String name) {
		WritePrecision p = WritePrecision.MS;
		
		// add random milliseconds to prevent overwrites
		Instant time = entry.getKey().plusMillis(RND.nextLong(100));
		Metric m = entry.getValue();
		
		// Get server and site from the metric itself
		String server = m.getServer();
		String anycastSite = m.getAnycastSite();
		
		String metricName = name;
		String[] parts = StringUtils.split(name, METRIC_SEPERATOR);
		if(parts != null && parts.length == 2) {
			metricName = parts[0];
		}

		if (m instanceof AvgMetric) {
			// make sure points are all saved a float and not mix of float and int, this will cause exception
			
			Point point = Point.measurement(metricName).setTimestamp(time.toEpochMilli(), p).setTags(m.getTags())
					.setTag("site", anycastSite)
					.setTag("server", server)
					.setTag("type", "avg")
					.setField("value", (float)m.getValue());

			points.add(point);

			point = Point.measurement(metricName).setTimestamp(time.toEpochMilli(), p)
					.setTag("server", server)
					.setTag("site", anycastSite)
					.setTag("type", "sample")
					.setField("value", (float)m.getSamples());

			points.add(point);

			point = Point.measurement(metricName).setTimestamp(time.toEpochMilli(), p)
					.setTag("server", server)
					.setTag("site", anycastSite)
					.setTag("type", "min")
					.setField("value", (float)((AvgMetric) m).getMin());

			points.add(point);

			point = Point.measurement(metricName).setTimestamp(time.toEpochMilli(), p)
					.setTag("server", server)
					.setTag("site", anycastSite)
					.setTag("type", "max")
					.setField("value", (float)((AvgMetric) m).getMax());

			points.add(point);
		} else {
			Point point = Point.measurement(metricName).setTimestamp(time.toEpochMilli(), p)
					.setTags(m.getTags())
					.setTag("site", anycastSite)
					.setTag("server", server)
					.setField("value", m.getValue());

			points.add(point);
			
		}
	}

}
