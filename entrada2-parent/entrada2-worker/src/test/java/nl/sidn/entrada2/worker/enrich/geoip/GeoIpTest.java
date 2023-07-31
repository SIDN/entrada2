package nl.sidn.entrada2.worker.enrich.geoip;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.net.InetAddress;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import com.maxmind.geoip2.model.CountryResponse;
import nl.sidn.entrada2.worker.service.emrich.geoip.GeoIPService;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class GeoIpTest {
  
  @Autowired 
  private GeoIPService geo;
  
  
  @Test
  public void testLoadGeoIpDatabases() throws Exception{
        
    assertNotNull(geo.getGeoReader());
    assertNotNull(geo.getAsnReader());
    assertTrue(geo.isGeoDbInitialised());
    assertFalse(geo.shouldUpdate());
    
    Optional<CountryResponse> oc = geo.lookupCountry(InetAddress.getByName("www.sidn.nl"));
    assertTrue(oc.isPresent());
  }

}
