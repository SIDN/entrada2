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
import java.util.Optional;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.load.DnsMetricValues;

/**
 * MetricManager is used to recreate metrics for DNS packets found in PCAP
 * files. The timestamp of a packets in the PCAP file is used when generating
 * the metrics and NOT the timestamp at the point in time when the packet was
 * read from the PCAP.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "management.influx.metrics.export", name = "uri", matchIfMissing = false)
public class HistoricalMetricManager {

	// dns stats
	public static final String METRIC_IMPORT_DNS_QUERY_COUNT = "dns.request";
	public static final String METRIC_IMPORT_DNS_RESPONSE_COUNT = "dns.response";

	public static final String METRIC_IMPORT_DNS_QTYPE = "dns.qtype";
	public static final String METRIC_IMPORT_DNS_RCODE = "dns.rcode";
	public static final String METRIC_IMPORT_DNS_OPCODE = "dns.opcode";
	public static final String METRIC_IMPORT_DNS_PROC_TIME = "dns.proc-time";

	// layer 4 stats
	public static final String METRIC_IMPORT_NETWORK_PROT = "network.protocol";

	public static final String METRIC_IMPORT_IP_COUNT = "ip.version";
	public static final String METRIC_IMPORT_COUNTRY_COUNT = "geo.country";

	public static final String METRIC_IMPORT_TCP_HANDSHAKE_RTT = "rtt";
	
	public static final Map<String, String> IP_V4_TAG_MAP = Map.of("version", "4");
	public static final Map<String, String> IP_V6_TAG_MAP = Map.of("version", "6");
	
	public static final Map<String, String> NETWORK_UDP_TAG_MAP = Map.of("protocol", "UDP");
	public static final Map<String, String> NETWORK_TCP_TAG_MAP = Map.of("protocol", "TCP");

	@Value("${management.influx.metrics.export.enabled:true}")
	protected boolean metricsEnabled;

	private static final Random RND = new Random();
	
	private Map<String, List<Metric>> metricCache = new HashMap<>(1000);

	private static final Map<String, String> EMPTY_MAP = new HashMap<>();

	@Autowired
	private WriteApi influxApi;

	private boolean isMetricsEnabled() {
		return metricsEnabled;
	}

	public void update(DnsMetricValues dmv) {

		if (!isMetricsEnabled() || dmv == null) {
			// do nothing
			return;
		}

		// this is the influx step size, using 1 second!
		Instant time = Instant.ofEpochSecond(dmv.time / 1000);

		if (dmv.dnsQuery) {
			update(METRIC_IMPORT_DNS_QUERY_COUNT, time, 1, true);
		}

		if (dmv.dnsResponse) {
			update(METRIC_IMPORT_DNS_RESPONSE_COUNT, time, 1, true);
		}

		if(dmv.dnsQtype != null) {
		update(METRIC_IMPORT_DNS_QTYPE + "." + dmv.dnsQtype.name(), METRIC_IMPORT_DNS_QTYPE, time, 1, true,
				Map.of("qtype", dmv.dnsQtype.name()));
		}
		
		update(METRIC_IMPORT_DNS_RCODE + "." + dmv.dnsRcode, METRIC_IMPORT_DNS_RCODE, time, 1, true,
				Map.of("rcode", String.valueOf(dmv.dnsRcode)));
		update(METRIC_IMPORT_DNS_OPCODE + "." + dmv.dnsOpcode, METRIC_IMPORT_DNS_OPCODE, time, 1, true,
				Map.of("opcode", String.valueOf(dmv.dnsOpcode)));

		if (StringUtils.isNotEmpty(dmv.country)) {
			update(METRIC_IMPORT_COUNTRY_COUNT + "." + dmv.country, METRIC_IMPORT_COUNTRY_COUNT, time, 1, true,
					Map.of("name", dmv.country));
		}

		if (dmv.ProtocolUdp) {
			update(METRIC_IMPORT_NETWORK_PROT + ".udp", METRIC_IMPORT_NETWORK_PROT, time, 1, true, NETWORK_UDP_TAG_MAP);
		} else {
			update(METRIC_IMPORT_NETWORK_PROT + ".tcp", METRIC_IMPORT_NETWORK_PROT, time, 1, true, NETWORK_TCP_TAG_MAP);
			if (dmv.hasTcpHandshake()) {
				update(METRIC_IMPORT_TCP_HANDSHAKE_RTT, time, dmv.tcpHandshake, false);
			}
		}

		if (dmv.ipV4) {
			update(METRIC_IMPORT_IP_COUNT + ".4", METRIC_IMPORT_IP_COUNT, time, 1, true, IP_V4_TAG_MAP);
		} else {
			update(METRIC_IMPORT_IP_COUNT + ".6", METRIC_IMPORT_IP_COUNT, time, 1, true, IP_V6_TAG_MAP);
		}
		
		
		update(METRIC_IMPORT_DNS_PROC_TIME, time, (int)dmv.procTime, false);
		
	}

	private void update(String name, Instant time, int value, boolean counter) {
		update(name, name, time, value, counter, EMPTY_MAP);
	}

	private void update(String name, String label, Instant time, int value, boolean counter, Map<String, String> tags) {

		List<Metric> metricValues = metricCache.get(name);

		if (metricValues == null) {
			metricValues = new ArrayList<Metric>();
			Metric m = createMetric(label, value, time, counter, tags);
			metricValues.add(m);
			metricCache.put(name, metricValues);
		} else {
			Optional<Metric> opt = lookupByLabelAndTime(label, time, metricValues);
			if (opt.isEmpty()) {
				Metric m = createMetric(label, value, time, counter, tags);
				metricValues.add(m);
			} else {
				opt.get().update(value);
			}
		}
	}

	private Optional<Metric> lookupByLabelAndTime(String label, Instant time, List<Metric> metricValues) {
		return metricValues.stream().filter(m -> m.getLabel().equals(label) && m.getTime().equals(time)).findFirst();
	}

	public static Metric createMetric(String label, int value, Instant timestamp, boolean counter,
			Map<String, String> labels) {
		if (counter) {
			return new SumMetric(label, value, timestamp, labels);
		}
		return new AvgMetric(label, value, timestamp, labels);
	}


	public void flush(String server, String anycastSite) {
		if (!isMetricsEnabled()) {
			// do nothing
			return;
		}

		try {
			metricCache.entrySet().stream().map(Entry::getValue).forEach(m -> send(m, server, anycastSite));
		} catch (Exception e) {
			// cannot connect connect to influxdb?
			log.error("Error sending metrics", e);
		}finally {
			metricCache.clear();
		}
		
		log.info("Flushed metrics");
	}

	private void send(List<Metric> metricValues, String server,  String anycastSite) {
		if (metricValues.isEmpty()) {
			// no metrics to send
			return;
		}
		metricValues.stream().forEach(e -> send(e, server, anycastSite, WritePrecision.NS));
	}

	private void send(Metric m, String server,  String anycastSite, WritePrecision p) {
		if(log.isDebugEnabled()) {
			log.debug("Metric: {}", m);
		}
		
		// add 1 nanosec to the last metric to prevent other pcap data from overwriting
		// the same second
		Instant time =  m.getTime().plusNanos(RND.nextLong(10000));

		if (m instanceof AvgMetric) {
			// make sure points are all saved a float and not mix of float and int, this will cause exception
			
			Point point = Point.measurement(m.getLabel()).time(time, p).addTags(m.getTags())
					.addTag("site", anycastSite)
					.addTag("server", server)
					.addTag("type", "avg").addField("value", (float)m.getValue());

			influxApi.writePoint(point);

			point = Point.measurement(m.getLabel()).time(time, p)
					.addTag("server", server)
					.addTag("site", anycastSite)
					.addTag("type", "sample")
					.addField("value", (float)m.getSamples());

			influxApi.writePoint(point);

			point = Point.measurement(m.getLabel()).time(time, p)
					.addTag("server", server)
					.addTag("site", anycastSite)
					.addTag("type", "min")
					.addField("value", (float)((AvgMetric) m).getMin());

			influxApi.writePoint(point);

			point = Point.measurement(m.getLabel()).time(time, p)
					.addTag("server", server)
					.addTag("site", anycastSite)
					.addTag("type", "max")
					.addField("value", (float)((AvgMetric) m).getMax());

			influxApi.writePoint(point);
		} else {
			Point point = Point.measurement(m.getLabel()).time(time, p)
					.addTags(m.getTags())
					.addTag("site", anycastSite)
					.addTag("server", server)
					.addField("value", m.getValue());

			influxApi.writePoint(point);
		}

	}

}
