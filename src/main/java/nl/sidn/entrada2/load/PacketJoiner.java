package nl.sidn.entrada2.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;

@Component
@Slf4j
@Getter
public class PacketJoiner {

	private static List<RowData> EMPTY_LIST = Collections.emptyList();
	
	private LinkedHashMap<RequestCacheKey, RequestCacheValue> requestCache = new LinkedHashMap<>();
	// keep list of active zone transfers
	private Cache<RequestCacheKey, Integer> activeZoneTransferCache;

	// stats counters
	private int counter = 0;
	private int matchedCounter = 0;
	private int requestPacketCounter = 0;
	private int responsePacketCounter = 0;
	private int cacheEvictionCounter = 0;
	
	private int maxRequestCacheSize;

	public PacketJoiner(@Value("${entrada.process.max-request-cache-size:10000}") int maxRequestCacheSize) {
		this.maxRequestCacheSize = maxRequestCacheSize;
		requestCache = LinkedHashMap.newLinkedHashMap(maxRequestCacheSize);
		activeZoneTransferCache = new Cache2kBuilder<RequestCacheKey, Integer>() {
		}.entryCapacity(300).build();
	}

	public List<RowData> join(Packet p) {
		if (p == Packet.NULL) {
			// ignore, but do purge first
			return EMPTY_LIST;
		}

		List<RowData> results = new ArrayList<>();

		counter++;

		if (counter % 100000 == 0 && log.isDebugEnabled()) {
			log.debug("Received {} packets to join", Integer.valueOf(counter));
		}

		// must be dnspacket
		if (isDNS(p)) {
			DNSPacket dnsPacket = (DNSPacket) p;

			if (dnsPacket.getMessages().isEmpty()) {
				// skip malformed packets
				log.debug("Packet contains no dns message, skipping...");
				return EMPTY_LIST;
			}

			for (Message msg : dnsPacket.getMessages()) {
				// put request into map until we find matching response, with a key based on:
				// query id,
				// qname, ip src, tcp/udp port add time for possible timeout eviction
				RowData d = null;
				if (msg.getHeader().getQr() == MessageType.QUERY) {
					d =  handDnsRequest(dnsPacket, msg);
				} else {
					d = handDnsResponse(dnsPacket, msg);				
				}
				
				if (d != null) {
					results.add(d);
				}
			}
			// clear the packet which may contain many dns messages
			dnsPacket.clear();

		}
		return results;
	}

	private boolean isDNS(Packet p) {
		return (p.getProtocol() == PacketFactory.PROTOCOL_TCP || p.getProtocol() == PacketFactory.PROTOCOL_UDP)
				&& (p.getSrcPort() == PcapReader.DNS_PORT || p.getDstPort() == PcapReader.DNS_PORT);
	}

	private boolean isXfr( Message msg) {
		return !msg.getQuestions().isEmpty() && (msg.getQuestions().get(0).getQType() == ResourceRecordType.AXFR
				|| msg.getQuestions().get(0).getQType() == ResourceRecordType.IXFR);
	}
	
	private RowData handDnsRequest(DNSPacket dnsPacket, Message msg) {
		requestPacketCounter++;
		// check for ixfr/axfr request
		if (isXfr(msg)) {

			if (log.isDebugEnabled()) {
				log.debug("Detected zone transfer for: " + dnsPacket.getFlow());
			}
			// keep track of ongoing zone transfer, we do not want to store all the response
			// packets for an ixfr/axfr.
			activeZoneTransferCache.put(
					new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getSrc(), dnsPacket.getSrcPort()),
					Integer.valueOf(0));
		}

		RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), qname(msg), dnsPacket.getSrc(),
				dnsPacket.getSrcPort());

		if (log.isDebugEnabled()) {
			log.debug("Insert into cache key: {}", key);
		}

		// put the query in the cache until we get a matching response
		requestCache.put(key, new RequestCacheValue(msg, dnsPacket));
		
		if(requestCache.size() > maxRequestCacheSize) {
			// cache is too big, remove last item and return to write to parquet
			cacheEvictionCounter++;
			
			if (cacheEvictionCounter % 100000 == 0) {
				log.info("Evicted " + cacheEvictionCounter + " DNS messages from cache");
			}
	
			Entry<RequestCacheKey, RequestCacheValue> lastEntry = requestCache.pollLastEntry();
			if(lastEntry != null) {
				return new RowData(lastEntry.getValue().getPacket(), lastEntry.getValue().getMessage(), null, null);
			}
		}
		
		// no expired cache data
		return null;
	}

	private RowData handDnsResponse(DNSPacket dnsPacket, Message msg) {
		responsePacketCounter++;
		
		RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getDst(),
				dnsPacket.getDstPort());
		
		// check for ixfr/axfr response, the query might be missing from the response
		// so we cannot use the qname for matching.
		if (activeZoneTransferCache.containsKey(key)) {
			if (log.isDebugEnabled()) {
				log.debug("Ignore {} zone transfer response(s)", Integer.valueOf(msg.getAnswer().size()));
			}
			// this response is part of an active zonetransfer.
			// only let the first response continue, reuse the "time" field of the
			// RequestKey to keep track of this.
			Integer ztResponseCounter = activeZoneTransferCache.get(key);
			if (ztResponseCounter.intValue() > 0) {
				// do not save this msg, drop it here, continue with next msg.
				return null;
			} else {
				// 1st response msg let it continue, add 1 to the map the indicate 1st resp msg
				// has been processed
				activeZoneTransferCache.put(key, Integer.valueOf(1));
			}
		}
		String qname = qname(msg);

		key = new RequestCacheKey(msg.getHeader().getId(), qname, dnsPacket.getDst(), dnsPacket.getDstPort());

		if (log.isDebugEnabled()) {
			log.debug("Get from cache key: " + key);
			//log.debug("request cache size before: " + requestCache.size());
		}

		RequestCacheValue request = requestCache.remove(key);
		// check to see if the request msg exists, at the start of the pcap there may be
		// missing queries

		if (request != null && request.getPacket() != null && request.getMessage() != null) {

			matchedCounter++;
			if (matchedCounter % 100000 == 0) {
				log.info("Matched " + matchedCounter + " DNS messages");
			}

			return new RowData(request.getPacket(), request.getMessage(), dnsPacket, msg);
		} else {
			// no request found, this could happen if the query was in previous pcap
			// and was not correctly decoded, or the request timed out before server
			// could send a response.

			if (log.isDebugEnabled()) {
				log.debug("Found no request for response, dst: " + dnsPacket.getDst() + " qname: " + qname);
			}

			if (qname != null) {
				return new RowData(null, null, dnsPacket, msg);
			}
		}

		return null;
	}

	/**
	 * get qname from request which is part of the cache lookup key
	 * 
	 * @param msg the DNS message
	 * @return the qname from the DNS question or null if not found.
	 */
	private String qname(Message msg) {
		String qname = null;
		if (!msg.getQuestions().isEmpty()) {
			qname = msg.getQuestions().get(0).getQName();
		}

		return qname;
	}

	public List<RowData> clearCache() {
		int purgeCounter = 0;

		List<RowData> unmatched = new ArrayList<>();

		for (RequestCacheValue cacheValue : requestCache.values()) {

			if (cacheValue.getMessage() != null && !cacheValue.getMessage().getQuestions().isEmpty()
					&& cacheValue.getMessage().getHeader().getQr() == MessageType.QUERY) {

				unmatched.add(new RowData(cacheValue.getPacket(), cacheValue.getMessage(), null, null));

				purgeCounter++;
			} else if (cacheValue.getMessage() != null
					&& cacheValue.getMessage().getHeader().getQr() == MessageType.RESPONSE) {

				unmatched.add(new RowData(null, null, cacheValue.getPacket(), cacheValue.getMessage()));

				purgeCounter++;
			}
		}

		log.info("* ---------------------------------------	*");
		log.info("*            PCAP DNS stats             	*");
		log.info("* ---------------------------------------	*");
		log.info("* Matched queries:       {}", matchedCounter);
		log.info("* Unmatched queries:     {}", purgeCounter);
		log.info("* Cache evicted queries: {}", cacheEvictionCounter);
		log.info("* ---------------------------------------	*");

		requestCache.clear();
		counter = 0;
		matchedCounter = 0;
		requestPacketCounter = 0;
		responsePacketCounter = 0;
		cacheEvictionCounter = 0;

		return unmatched;
	}

}
