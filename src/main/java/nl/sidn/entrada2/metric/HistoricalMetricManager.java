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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Scope;
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
 */
@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
	
	public static final Map<String, String> IP_V4_TAG_MAP = Map.of("version", "4");
	public static final Map<String, String> IP_V6_TAG_MAP = Map.of("version", "6");
	
	public static final Map<String, String> NETWORK_UDP_TAG_MAP = Map.of("protocol", "UDP");
	public static final Map<String, String> NETWORK_TCP_TAG_MAP = Map.of("protocol", "TCP");

	private static final Random RND = new Random();

	// name -> time -> value
	private Map<String, Map<Instant, Metric>> metricCache = new HashMap<>();

	private static final Map<String, String> EMPTY_MAP = new HashMap<>();

	@Autowired(required = false)
	private InfluxDBClient influxClient;

	public void update(DnsMetricValues dmv) {

		if (dmv == null) {
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
		update(METRIC_IMPORT_DNS_QTYPE + METRIC_SEPERATOR + dmv.dnsQtype.name(), time, 1, true,
				Map.of("qtype", dmv.dnsQtype.name()));
		}
		
		update(METRIC_IMPORT_DNS_RCODE + METRIC_SEPERATOR + dmv.dnsRcode, time, 1, true,
				Map.of("rcode", String.valueOf(dmv.dnsRcode)));
		update(METRIC_IMPORT_DNS_OPCODE + METRIC_SEPERATOR + dmv.dnsOpcode, time, 1, true,
				Map.of("opcode", String.valueOf(dmv.dnsOpcode)));

		if (StringUtils.isNotEmpty(dmv.country)) {
			update(METRIC_IMPORT_COUNTRY_COUNT + METRIC_SEPERATOR + dmv.country, time, 1, true,
					Map.of("name", dmv.country));
		}

		if (dmv.ProtocolUdp) {
			update(METRIC_IMPORT_NETWORK_PROT + METRIC_SEPERATOR + "udp", time, 1, true, NETWORK_UDP_TAG_MAP);
		} else {
			update(METRIC_IMPORT_NETWORK_PROT + METRIC_SEPERATOR + "tcp", time, 1, true, NETWORK_TCP_TAG_MAP);
			if (dmv.hasTcpHandshake()) {
				update(METRIC_IMPORT_TCP_HANDSHAKE_RTT, time, dmv.tcpHandshake, false);
			}
		}

		if (dmv.ipV4) {
			update(METRIC_IMPORT_IP_COUNT + METRIC_SEPERATOR + "4", time, 1, true, IP_V4_TAG_MAP);
		} else {
			update(METRIC_IMPORT_IP_COUNT + METRIC_SEPERATOR + "6", time, 1, true, IP_V6_TAG_MAP);
		}
		
		
		update(METRIC_IMPORT_DNS_PROC_TIME, time, (int)dmv.procTime, false);
		
	}

	private void update(String name, Instant time, int value, boolean counter) {
		update(name, time, value, counter, EMPTY_MAP);
	}

	private void update(String name, Instant time, int value, boolean counter, Map<String, String> tags) {

		Map<Instant, Metric> metricValues = metricCache.get(name);

		if (metricValues == null) {
			metricValues = new LinkedHashMap<>();
			Metric m = createMetric(value, counter, tags);
			metricValues.put(time, m);
			metricCache.put(name, metricValues);
		} else {
			Metric m = metricValues.get(time);
			
			if (m != null) {
				m.update(value);
			}else {
				metricValues.put(time, createMetric(value, counter, tags));
			}
		}
	}

	public static Metric createMetric(int value, boolean counter, Map<String, String> labels) {
		if (counter) {
			return new SumMetric(value, labels);
		}
		return new AvgMetric(value, labels);
	}


	public void flush(String server, String anycastSite) {

		try {
			for(Entry<String, Map<Instant, Metric>> entry : metricCache.entrySet()) {
				entry.getValue().entrySet().stream().forEach(m -> send(m, entry.getKey(), server, anycastSite));
			}
		} catch (Exception e) {
			// cannot connect connect to influxdb?
			log.error("Error sending metrics", e);
		}finally {
			metricCache.clear();
		}
		
		log.info("Flushed metrics");
	}

	private void send(Entry<Instant, Metric> entry, String name, String server, String anycastSite) {
		if(log.isDebugEnabled()) {
			log.debug("Metric: {} value: {}", name, entry);
		}
		
		WritePrecision p =  WritePrecision.NS;
		
		// add 1 nanosec to the last metric to prevent other pcap data from overwriting
		// the same second
		Instant time = entry.getKey().plusNanos(RND.nextLong(10000));
		Metric m = entry.getValue();
		
		String metricName = name;
		String[] parts = StringUtils.split(name, METRIC_SEPERATOR);
		if(parts != null && parts.length == 2) {
			metricName = parts[0];
		}

		if (m instanceof AvgMetric) {
			// make sure points are all saved a float and not mix of float and int, this will cause exception
			
			Point point = Point.measurement(metricName).setTimestamp(time.getEpochSecond(), p).setTags(m.getTags())
					.setTag("site", anycastSite)
					.setTag("server", server)
					.setTag("type", "avg")
					.setField("value", (float)m.getValue());

			influxClient.writePoint(point);

			point = Point.measurement(metricName).setTimestamp(time.getEpochSecond(), p)
					.setTag("server", server)
					.setTag("site", anycastSite)
					.setTag("type", "sample")
					.setField("value", (float)m.getSamples());

			influxClient.writePoint(point);

			point = Point.measurement(metricName).setTimestamp(time.getEpochSecond(), p)
					.setTag("server", server)
					.setTag("site", anycastSite)
					.setTag("type", "min")
					.setField("value", (float)((AvgMetric) m).getMin());

			influxClient.writePoint(point);

			point = Point.measurement(metricName).setTimestamp(time.getEpochSecond(), p)
					.setTag("server", server)
					.setTag("site", anycastSite)
					.setTag("type", "max")
					.setField("value", (float)((AvgMetric) m).getMax());

			influxClient.writePoint(point);
		} else {
			Point point = Point.measurement(metricName).setTimestamp(time.getEpochSecond(), p)
					.setTags(m.getTags())
					.setTag("site", anycastSite)
					.setTag("server", server)
					.setField("value", m.getValue());

			influxClient.writePoint(point);
		}

	}

}
