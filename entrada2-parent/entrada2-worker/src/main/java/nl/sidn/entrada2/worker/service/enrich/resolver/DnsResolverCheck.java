package nl.sidn.entrada2.worker.service.enrich.resolver;

import java.net.InetAddress;
import java.util.List;

public interface DnsResolverCheck {

  void load();

  String getName();

  boolean match(String address, InetAddress inetAddress);

 // int getMatcherCount();

 // void done();
  
  void download();
  
  List<String> fetch();
  
  List<String> loadFromFile();
  
  String getFilename();
}
