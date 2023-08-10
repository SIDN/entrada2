package nl.sidn.entrada2.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import feign.Retryer;

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


