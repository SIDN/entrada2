package nl.sidn.entrada2.service.enrich.geoip;

import java.net.InetAddress;
import org.springframework.stereotype.Component;

import com.maxmind.geoip2.model.CountryResponse;

import nl.sidn.entrada2.load.FieldEnum;
import nl.sidn.entrada2.service.enrich.AddressEnrichment;

@Component
public class CountryEnrichment implements AddressEnrichment {

  private GeoIPService geoLookup;

  public CountryEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;
  }


  /**
   * Lookup country for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with country if found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {

    return geoLookup.lookupCountry(inetAddress).map(CountryResponse::country)
        .map(country -> country.isoCode())
        .orElse(null);
  }


  @Override
  public String getColumn() {
    return FieldEnum.ip_geo_country.name();
  }


}
