package nl.sidn.entrada2.worker.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import feign.Response;
import feign.Retryer;
import feign.codec.ErrorDecoder;

@Configuration
public class FeignConfig {

  @Bean
  @ConditionalOnMissingBean
  Retryer feignRetryer() {
    return Retryer.NEVER_RETRY;
  }

//  @Bean
//  ErrorDecoder errorDecoder() {
//    return new CustomErrorDecoder();
//  }
//
//  public class CustomErrorDecoder implements ErrorDecoder {
//
//    @Override
//    public Exception decode(String methodKey, Response response) {
//      return new Exception("Error");
//    }
//  }


}


