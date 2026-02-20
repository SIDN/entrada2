package nl.sidnlabs.entrada2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import nl.sidn.entrada2.load.PacketJoiner;
import nl.sidn.entrada2.load.RowData;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.Packet;

public class JoinerTest extends AbstractTest {

    @Test
    void testJoinerOkPcap() {
        PcapReader reader = createReaderFor("pcap/sidnlabs-test-query-response-ok.pcap");
        PacketJoiner joiner = new PacketJoiner(10000);

        // Join all packets from the pcap into request/response pairs
        List<Packet> packets = reader.stream().skip(0).collect(Collectors.toList());

        assertEquals(2, packets.size(), "Expected 2 packets from the pcap file");

        List<RowData> rowDataList = packets.stream()
                .flatMap(p -> joiner.join(p).stream())
                .collect(Collectors.toList());

         assertEquals(1, rowDataList.size(), "Expected 1 RowData from the pcap file");
    }

   
}
