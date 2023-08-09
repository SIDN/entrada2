package nl.sidn.entrada2.worker.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
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


  private Map<RequestCacheKey, RequestCacheValue> requestCache = new HashMap<>();
  // keep list of active zone transfers
  private Map<RequestCacheKey, Integer> activeZoneTransfers = new HashMap<>();

  // stats counters
  private int counter = 0;
  private int matchedCounter = 0;
  private int requestPacketCounter = 0;
  private int responsePacketCounter = 0;

  private final MeterRegistry registry;


  public PacketJoiner(MeterRegistry registry) {
    this.registry = registry;
  }


  public List<RowData> join(Packet p) {
    if (p == Packet.NULL) {
      // ignore, but do purge first
      return Collections.emptyList();
    }

    if (p == Packet.LAST) {
      // ignore, but do purge first
      logStats();
      return clearCache();
    }
    List<RowData> results = new ArrayList<>();

    counter++;
    registry.counter("entrada.pcap.packets.processed").increment();

    if (counter % 100000 == 0 && log.isDebugEnabled()) {
      log.debug("Received {} packets to join", Integer.valueOf(counter));
    }

    // must be dnspacket
    if (isDNS(p)) {
      DNSPacket dnsPacket = (DNSPacket) p;

      if (dnsPacket.getMessages().isEmpty()) {
        // skip malformed packets
        log.debug("Packet contains no dns message, skipping...");
        return Collections.emptyList();
      }

      for (Message msg : dnsPacket.getMessages()) {
        // put request into map until we find matching response, with a key based on: query id,
        // qname, ip src, tcp/udp port add time for possible timeout eviction
        if (msg.getHeader().getQr() == MessageType.QUERY) {
          handDnsRequest(dnsPacket, msg, p.getFilename());
        } else {
          RowData d = handDnsResponse(dnsPacket, msg, p.getFilename());
          if (d != null) {
            results.add(d);
          }
        }
      }
      // clear the packet which may contain many dns messages
      dnsPacket.clear();

    }
    return results;
    // } // end of dns packet
  }

  private boolean isDNS(Packet p) {
    return (p.getProtocol() == PacketFactory.PROTOCOL_TCP
        || p.getProtocol() == PacketFactory.PROTOCOL_UDP) &&
        (p.getSrcPort() == PcapReader.DNS_PORT || p.getDstPort() == PcapReader.DNS_PORT);
  }

  private void handDnsRequest(DNSPacket dnsPacket, Message msg, String fileName) {
    requestPacketCounter++;
    registry.counter("entrada.dns.packets.requests").increment();
    // check for ixfr/axfr request
    if (!msg.getQuestions().isEmpty()
        && (msg.getQuestions().get(0).getQType() == ResourceRecordType.AXFR
            || msg.getQuestions().get(0).getQType() == ResourceRecordType.IXFR)) {

      if (log.isDebugEnabled()) {
        log.debug("Detected zone transfer for: " + dnsPacket.getFlow());
      }
      // keep track of ongoing zone transfer, we do not want to store all the response
      // packets for an ixfr/axfr.
      activeZoneTransfers
          .put(new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getSrc(),
              dnsPacket.getSrcPort(), 0), Integer.valueOf(0));
    }

    RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), qname(msg),
        dnsPacket.getSrc(), dnsPacket.getSrcPort(), dnsPacket.getTsMilli());

    if (log.isDebugEnabled()) {
      log.info("Insert into cache key: " + key);
    }

    // put the query in the cache until we get a matching response
    requestCache.put(key, new RequestCacheValue(msg, dnsPacket));
  }

  private RowData handDnsResponse(DNSPacket dnsPacket, Message msg, String fileName) {
    responsePacketCounter++;
    registry.counter("entrada.dns.packets.responses").increment();
    // try to find the request

    // check for ixfr/axfr response, the query might be missing from the response
    // so we cannot use the qname for matching.
    RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getDst(),
        dnsPacket.getDstPort(), 0);
    if (activeZoneTransfers.containsKey(key)) {
      if (log.isDebugEnabled()) {
        log.debug("Ignore {} zone transfer response(s)", Integer.valueOf(msg.getAnswer().size()));
      }
      // this response is part of an active zonetransfer.
      // only let the first response continue, reuse the "time" field of the RequestKey to
      // keep track of this.
      Integer ztResponseCounter = activeZoneTransfers.get(key);
      if (ztResponseCounter.intValue() > 0) {
        // do not save this msg, drop it here, continue with next msg.
        return null;
      } else {
        // 1st response msg let it continue, add 1 to the map the indicate 1st resp msg
        // has been processed
        activeZoneTransfers.put(key, Integer.valueOf(1));
      }
    }
    String qname = qname(msg);

    key = new RequestCacheKey(msg.getHeader().getId(), qname, dnsPacket.getDst(),
        dnsPacket.getDstPort(), 0);

    if (log.isDebugEnabled()) {
      log.debug("Get from cache key: " + key);
      log.debug("request cache size before: " + requestCache.size());
    }

    RequestCacheValue request = requestCache.remove(key);
    // check to see if the request msg exists, at the start of the pcap there may be
    // missing queries
    if (log.isDebugEnabled()) {
      log.debug("request cache size after: " + requestCache.size());
    }

    if (request != null && request.getPacket() != null && request.getMessage() != null) {

      matchedCounter++;
      if (matchedCounter % 100000 == 0 && log.isDebugEnabled()) {
        log.debug("Matched " + matchedCounter + " packets");
      }

      // pushRow(
      return new RowData(request.getPacket(), request.getMessage(), dnsPacket, msg);
      // );

    } else {
      // no request found, this could happen if the query was in previous pcap
      // and was not correctly decoded, or the request timed out before server
      // could send a response.

      if (log.isDebugEnabled()) {
        log.debug("Found no request for response, dst: " + dnsPacket.getDst() + " qname: " + qname);
      }

      if (qname != null) {
        // pushRow(
        return new RowData(null, null, dnsPacket, msg);
        // );
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

  public void logStats() {
    log.info("-------------- Done processing pcap file -----------------");
    log.info("{} total DNS messages: ", Integer.valueOf(counter));
    log.info("{} requests: ", Integer.valueOf(requestPacketCounter));
    log.info("{} responses: ", Integer.valueOf(responsePacketCounter));
  }

  public Map<RequestCacheKey, RequestCacheValue> getRequestCache() {
    return requestCache;
  }

  public void setRequestCache(Map<RequestCacheKey, RequestCacheValue> requestCache) {
    this.requestCache = requestCache;
  }
  
  private List<RowData> clearCache() {
    int purgeCounter = 0;


    List<RowData> unmatched = new ArrayList<>();

    for (RequestCacheValue cacheValue : requestCache.values()) {
      
      if (cacheValue.getMessage() != null && !cacheValue.getMessage().getQuestions().isEmpty()
          && cacheValue.getMessage().getHeader().getQr() == MessageType.QUERY) {

        unmatched
            .add(new RowData(cacheValue.getPacket(), cacheValue.getMessage(), null, null));

        purgeCounter++;
      }else if (cacheValue.getMessage() != null && cacheValue.getMessage().getHeader().getQr() == MessageType.RESPONSE) {

        unmatched
            .add(new RowData(null, null, cacheValue.getPacket(), cacheValue.getMessage()));

        purgeCounter++;      
      }
    }
    
    log.info("Not matched query's with rcode -1 (no response/request): {}", Integer.valueOf(purgeCounter));

    registry.counter("entrada.dns.packets.expired").increment(purgeCounter);
    requestCache.clear();
    activeZoneTransfers.clear();
    
    counter = 0;
    matchedCounter = 0;
    requestPacketCounter = 0;
    responsePacketCounter = 0;
    
    return unmatched;
  }

}
