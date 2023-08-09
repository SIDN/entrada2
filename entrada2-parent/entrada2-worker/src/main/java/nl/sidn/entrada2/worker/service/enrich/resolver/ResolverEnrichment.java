package nl.sidn.entrada2.worker.service.enrich.resolver;

import java.net.InetAddress;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import nl.sidn.entrada2.worker.service.enrich.AddressEnrichment;

@Component
public class ResolverEnrichment implements AddressEnrichment {

  @Autowired
  private List<DnsResolverCheck> resolverChecks;

  public ResolverEnrichment() {
  }

  /**
   * Check if the IP address is linked to a known open resolver operator
   * 
   * @param address IP address to perform lookup with
   * @return Optional with name of resolver operator, empty if no match found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {

    for (DnsResolverCheck check : resolverChecks) {
      if (check.match(address, inetAddress)) {
        String value = check.getName();
        return value;
      }
    }

    return null;
  }


  @Override
  public String getColumn() {
    return "pub_resolver";
  }


}
