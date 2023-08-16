package nl.sidn.entrada2.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import feign.Client;
import feign.Retryer;

@Configuration
public class FeignConfig {

  @Bean
  @ConditionalOnMissingBean
  Retryer feignRetryer() {
    return Retryer.NEVER_RETRY;
  }

}


