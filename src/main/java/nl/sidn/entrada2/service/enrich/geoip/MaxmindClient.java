package nl.sidn.entrada2.service.enrich.geoip;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import feign.Response;

@FeignClient(name="maxmindClient")
public interface MaxmindClient {
  
  @RequestMapping(method = RequestMethod.GET)
  Response getDatabase(@RequestParam("edition_id") String editionId, @RequestParam("license_key") String licenseKey);
  
  @RequestMapping(method = RequestMethod.HEAD)
  ResponseEntity<Void> peek(@RequestParam("edition_id") String editionId, @RequestParam("license_key") String licenseKey);

}
