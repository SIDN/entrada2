package nl.sidn.entrada2.service.enrich.resolver;

import java.net.InetAddress;
import java.util.List;

public interface DnsResolverCheck {

  void download();

  String getName();

  boolean match(String address, InetAddress inetAddress);

  void update();
  
  List<String> fetch();
  
  List<String> loadFromFile();
  
  String getFilename();
}
