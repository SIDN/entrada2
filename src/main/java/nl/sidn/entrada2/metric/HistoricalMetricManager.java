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
@ConditionalOnProperty(prefix = "entrada.metric.influxdb", name = "url", matchIfMissing = false)
public class HistoricalMetricManager {

	// dns stats
	public static final String METRIC_IMPORT_DNS_QUERY_COUNT = "dns.request";
	public static final String METRIC_IMPORT_DNS_RESPONSE_COUNT = "dns.response";

	public static final String METRIC_IMPORT_DNS_QTYPE = "dns.qtype";
	public static final String METRIC_IMPORT_DNS_RCODE = "dns.rcode";
	public static final String METRIC_IMPORT_DNS_OPCODE = "dns.opcode";

	// layer 4 stats
	public static final String METRIC_IMPORT_TCP_COUNT = "tcp";
	public static final String METRIC_IMPORT_UDP_COUNT = "udp";

	public static final String METRIC_IMPORT_IP_V4_COUNT = "ip.version";
	public static final String METRIC_IMPORT_IP_V6_COUNT = "ip.version";

	public static final String METRIC_IMPORT_COUNTRY_COUNT = "geo.country";

	public static final String METRIC_IMPORT_TCP_HANDSHAKE_RTT = "tcp.rtt";

	@Value("${entrada.metrics.enabled:true}")
	protected boolean metricsEnabled;

	private Map<String, List<Metric>> metricCache = new HashMap<>(1000);

	@Value("${entrada.metrics.influxdb.env}")
	private String env;

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
			update(METRIC_IMPORT_UDP_COUNT, time, 1, true);
		} else {
			update(METRIC_IMPORT_TCP_COUNT, time, 1, true);
			if (dmv.hasTcpHandshake()) {
				update(METRIC_IMPORT_TCP_HANDSHAKE_RTT, time, dmv.tcpHandshake, false);
			}
		}

		if (dmv.ipV4) {
			update(METRIC_IMPORT_IP_V4_COUNT + ".4", METRIC_IMPORT_IP_V4_COUNT, time, 1, true, Map.of("version", "4"));
		} else {
			update(METRIC_IMPORT_IP_V6_COUNT + ".6", METRIC_IMPORT_IP_V6_COUNT, time, 1, true, Map.of("version", "6"));
		}
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

	/**
	 * Uses a threshhold to determine if the value should be sent to graphite low
	 * values may indicate trailing queries in later pcap files. duplicate
	 * timestamps get overwritten by graphite and only the last timestamp value is
	 * used by graphite.
	 */
	public void flush(String server) {
		if (!isMetricsEnabled()) {
			// do nothing
			return;
		}

		try {
			metricCache.entrySet().stream().map(Entry::getValue).forEach(m -> send(m, server));
		} catch (Exception e) {
			// cannot connect connect to influxdb?
			log.error("Error sending metrics", e);
		}

		metricCache.clear();
		log.info("Flushed metrics");
	}

	private void send(List<Metric> metricValues, String server) {
		if (metricValues.isEmpty()) {
			// no metrics to send
			return;
		}

		// add 1 nanosec to the last metric to prevent the next pcap from overwriting
		// the same second
		Metric last = metricValues.get(metricValues.size() - 1);
		last.setTime(last.getTime().plusNanos(1));

		metricValues.stream().forEach(e -> send(e, server, WritePrecision.S));
	}

	private void send(Metric m, String server, WritePrecision p) {

		if (m instanceof AvgMetric) {

			Point point = Point.measurement(m.getLabel() + ".avg").time(m.getTime(), p).addTags(m.getTags())
					.addTag("server", server).addTag("env", env).addField("value", m.getValue());

			influxApi.writePoint(point);

			point = Point.measurement(m.getLabel() + ".samples").time(m.getTime(), p).addTag("server", server)
					.addTag("env", env).addField("value", m.getSamples());

			influxApi.writePoint(point);

			point = Point.measurement(m.getLabel() + ".min").time(m.getTime(), p).addTag("server", server)
					.addTag("env", env).addField("value", ((AvgMetric) m).getMin());

			influxApi.writePoint(point);

			point = Point.measurement(m.getLabel() + ".max").time(m.getTime(), p).addTag("server", server)
					.addTag("env", env).addField("value", ((AvgMetric) m).getMax());

			influxApi.writePoint(point);
		} else {
			Point point = Point.measurement(m.getLabel()).time(m.getTime(), p).addTags(m.getTags())
					.addTag("server", server).addTag("env", env).addField("value", m.getValue());

			influxApi.writePoint(point);
		}

	}

}
