package nl.sidn.entrada2.service.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import com.maxmind.geoip2.model.AsnResponse;
import nl.sidn.entrada2.service.enrich.AddressEnrichment;

@Component
@ConditionalOnProperty( name = "entrada.mode", havingValue = "worker")
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

    Optional<? extends AsnResponse> r = geoLookup.lookupASN(inetAddress);
    if (r.isPresent()) {
      return r.get().getAutonomousSystemOrganization();
    }

    return null;
  }

  @Override
  public String getColumn() {
    return "asn_organisation";
  }


}
