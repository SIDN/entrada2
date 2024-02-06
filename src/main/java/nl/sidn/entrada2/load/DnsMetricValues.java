package nl.sidn.entrada2.load;

import lombok.Builder;
import lombok.Value;
import nl.sidnlabs.dnslib.types.ResourceRecordType;

@Value
@Builder
public class DnsMetricValues {

  public long time;
  public boolean dnsQuery;
  public boolean dnsResponse;
  public ResourceRecordType dnsQtype;
  public int dnsRcode;
  public int dnsOpcode;

  public boolean ProtocolUdp;

  public boolean ipV4;
  public String country;

  @Builder.Default
  public int tcpHandshake = -1;


  public boolean hasTcpHandshake() {
	  return tcpHandshake != -1;
  }
}
