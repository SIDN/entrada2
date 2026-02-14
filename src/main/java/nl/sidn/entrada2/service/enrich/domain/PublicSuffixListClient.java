package nl.sidn.entrada2.service.enrich.domain;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import feign.Response;

@FeignClient(name="publicSuffixListClient")
public interface PublicSuffixListClient {
  
  @RequestMapping(method = RequestMethod.GET)
  Response getList();
  
  @RequestMapping(method = RequestMethod.HEAD)
  ResponseEntity<Void> peek();

}
