package nl.sidn.entrada2.service.enrich;

import java.net.InetAddress;

public interface AddressEnrichment {

  String match(String address, InetAddress inetAddress);

  String getColumn();

}
