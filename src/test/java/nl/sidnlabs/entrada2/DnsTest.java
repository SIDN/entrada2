package nl.sidnlabs.entrada2;


import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Packet;

public class DnsTest extends AbstractTest {

	
  @Test
  public void testQueryResponseOk() {
    // this pcap contains a single dns response that uses 2 ip fragments
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-query-response-ok.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    // query
    DNSPacket dp = (DNSPacket) pckts.get(0);
    assertEquals(1, dp.getMessageCount());   
    assertEquals(MessageType.QUERY, dp.getMessages().get(0).getHeader().getQr());

    // response
    dp = (DNSPacket) pckts.get(1);
    assertEquals(1, dp.getMessageCount());   
    assertEquals(MessageType.RESPONSE, dp.getMessages().get(0).getHeader().getQr());

  }

  
}
