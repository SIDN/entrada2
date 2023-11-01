package nl.sidnlabs.entrada2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.common.net.InternetDomainName;

import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.Packet;

public class StringTest extends AbstractTest {

	@Test
	public void testDomainname() {
		InternetDomainName d = InternetDomainName.from("wwww.sidn.nl");
		assertEquals("nl", d.registrySuffix().toString());
		
		d = InternetDomainName.from("wwww.sidn.co.uk");
		assertEquals("co.uk", d.registrySuffix().toString());
	}

	  @Test
	  public void testQnameNonAscii() {

	    PcapReader reader = createReaderFor("pcap/sidnlabs-test-non-ascii-in-qname.example.pcap");
	    List<Packet> pckts = reader.stream().collect(Collectors.toList());
	    assertEquals(2, pckts.size());
	  }

	  
	  
//	@Test
//	public void TestEncodingsize() throws Exception{
//		System.out.println(Charset.defaultCharset());
//		String t = new String("www.sidnlabs.nl".getBytes(), StandardCharsets.UTF_8);
//		System.out.println("bytes: " + t.getBytes().length);
//		
//		String asciiEncodedString = new String("www.sidnlabs.nl".getBytes(), StandardCharsets.US_ASCII);
//		
//		System.out.println("ascii: " + asciiEncodedString);
//		System.out.println("bytes: " + asciiEncodedString.getBytes().length);
//		
//
//		String utf16 = new String("www.sidnlabs.nl".getBytes(), StandardCharsets.UTF_16);
//		
//		System.out.println("bytes: " + utf16.getBytes().length);
//		System.out.println("bytes: " + new String(utf16.getBytes(StandardCharsets.US_ASCII), StandardCharsets.US_ASCII));
//	}

}
