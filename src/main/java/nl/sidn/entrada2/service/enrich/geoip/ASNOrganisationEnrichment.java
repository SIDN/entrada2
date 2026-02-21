package nl.sidn.entrada2.service.enrich.geoip;

import java.net.InetAddress;
import org.springframework.stereotype.Component;

import nl.sidn.entrada2.load.FieldEnum;
import nl.sidn.entrada2.service.enrich.AddressEnrichment;

@Component
public class ASNOrganisationEnrichment implements AddressEnrichment {

  private GeoIPService geoLookup;

  public ASNOrganisationEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;
  }

  /**
   * Lookup ASN for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with ASN if found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {
    return geoLookup.lookupASNOrg(inetAddress).orElse(null);
  }

  @Override
  public String getColumn() {
    return FieldEnum.ip_asn_org.name();
    }


}
